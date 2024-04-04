import argparse
import contextlib
import datetime
import glob
import json
import math
import os
import sys
import tempfile
import time
import yaml

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, Queue
from threading import Thread
from time import sleep
from typing import Dict, Optional
from urllib.parse import urlparse, quote, unquote

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, AzureCliCredential

from loguru import logger

from marker.convert import convert_single_pdf, get_length_of_text
from marker.models import load_all_models
from marker.settings import settings
from marker.logger import configure_logging

from obsidian.core.azure.batch import utils as batch_utils
from obsidian.core.data import azcopy
from obsidian.core.utils import gen_obsidian_init_command, gen_pip_command, gen_apt_command

g_config = None
g_sas_token = None

configure_logging()
logger.add("pdfmarker.log", rotation="10 MB", retention="10 days", level="INFO")

@contextlib.contextmanager
def timer(name):
    t0 = time.monotonic()
    try:
        yield
    finally:
        t1 = time.monotonic()
        logger.info(f'[Timer({name})] {t1 - t0:.3f} s')

class QuickDict(dict):
    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

def get_config(config_file) -> QuickDict:
    if os.path.exists(config_file):
        with open(config_file, "r", encoding="utf-8") as f_conf:
            config_data = QuickDict(yaml.safe_load(f_conf))
    else:
        raise FileNotFoundError(f"configuration file {os.path.abspath(config_file)} not found!")
    
    return config_data

ENV_OFFSET = 'MY_JOB_OFFSET'
ENV_LENGTH = 'MY_JOB_LENGTH'
ENV_INDEX_URL = 'MY_INDEX_URL'
ENV_SRC_URL = 'MY_SRC_URL'
ENV_DST_URL = 'MY_DST_URL'
ENV_KEYVAULT_NAME = 'MY_KEYVAULT_NAME'
ENV_IDENTITY_ID = 'MY_IDENTITY_ID'
ENV_SECRET_NAME = 'MY_SECRET_NAME'

def get_config_fromenv():
    return QuickDict({
        "index_url": os.environ[ENV_INDEX_URL],
        "src_url": os.environ[ENV_SRC_URL],
        "dst_url": os.environ[ENV_DST_URL],
        "keyvault_name": os.environ[ENV_KEYVAULT_NAME],
        "identity_id": os.environ[ENV_IDENTITY_ID],
        "secret_name": os.environ[ENV_SECRET_NAME],
        "offset": int(os.environ[ENV_OFFSET]),
        "length": int(os.environ[ENV_LENGTH]),
    })

def get_blob_sas(identity_id: str, keyvault_name: str, secret_name: str):
    KVUri = f"https://{keyvault_name}.vault.azure.net/"

    credential = DefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
        managed_identity_client_id=identity_id
    )
    client = SecretClient(vault_url=KVUri, credential=credential)

    return client.get_secret(secret_name)

def monitor_queue(queue):
    while True:
        sleep(0.1)  # Check the queue every 100ms
        message = queue.get()
        if message is None:  # Check for sentinel value
            break
        print(message)

def get_file_count(url):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as tmp_dir:
        index_file = azcopy.copy(url, tmp_dir)
        with open(index_file) as fin:
            return sum(1 for _ in fin)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def pre_process(fname: str, out_folder: str):
    # fname: A/B/[10.1002]sample.pdf
    in_url = azcopy.join_path(g_config.src_url, fname) # https://abc.blob.core.windows.net/xxx/A/B/[10.1002]sample.pdf
    return azcopy.copy(in_url, out_folder) # .../tmp2p_abcd/[10.1002]sample.pdf

def post_process(full_text, out_filename, out_url):
    basename = os.path.basename(out_filename)
    if len(full_text.strip()) > 0:
        with open(out_filename, "w+", encoding='utf-8') as f:
            f.write(full_text)
        
        logger.info(f"Uploading {unquote(basename)}")
        azcopy.upload(out_filename, out_url)

        # ignore the metadata for now
        # with open(out_meta_filename, "w+") as f:
        #     f.write(json.dumps(out_metadata, indent=4))
    else:
        logger.info(f"Empty file: {unquote(basename)}.  No valid convert result")

def process_single_pdf(
    fname: str,
    out_folder: str, 
    models,
    min_length: Optional[int] = None,
    queue = None
    ):
    
    # fname: A/B/[10.1002]sample.pdf
    try:
        local_file = pre_process(fname, out_folder) # .../tmp2p_abcd/[10.1002]sample.pdf        

        local_file_noext = os.path.splitext(local_file)[0] # .../tmp2p_abcd/[10.1002]sample
        out_filename = local_file_noext + ".md" # .../tmp2p_abcd/[10.1002]sample.md
        # out_meta_filename = local_file_noext + "_meta.json" # .../tmp2p_abcd/[10.1002]sample_meta.json

        out_relative_path = os.path.splitext(fname)[0] + ".md" # A/B/[10.1002]sample.md
        out_url = azcopy.join_path(g_config.dst_url, out_relative_path) # https://abc.blob.core.windows.net/yyy/A/B/[10.1002]sample.md

        logger.info(f"Converting {unquote(fname)}")

        # Skip trying to convert files that don't have a lot of embedded text
        # This can indicate that they were scanned, and not OCRed properly
        # Usually these files are not recent/high-quality
        if min_length:
            length = get_length_of_text(local_file)
            if length < min_length:
                return

        full_text, out_metadata = convert_single_pdf(local_file, models)
        post_process(full_text, out_filename, out_url)
    except Exception as e:
        logger.info(f"Error converting {unquote(local_file)}")
        logger.exception(e)

def process_minibatch(
    minibatch: list,
    out_folder: str, 
    min_length: Optional[int] = None,
    queue = None
    ):
    with timer("load_models"):
        models = load_all_models()
    
    logger.info(f"minibatch of {len(minibatch)} files")
    
    with timer("process_minibatch"):
        for fname in minibatch:
            with timer("process_single_pdf"):
                process_single_pdf(fname, out_folder, models, min_length, queue)

def run(offset, length, min_length, workers):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as tmp_dir:
        logger.info(f"run(offset={offset}, length={length}, min_length={min_length}, workers={workers})")
        logger.info(f'Using temp dir: {tmp_dir}')
        
        logger.info(f'Get file list from Azure storage...')

        with timer("get_index"):
            index_file = azcopy.copy(g_config.index_url, tmp_dir)
            with open(index_file) as fin:
                urls = [quote(url.strip()) for url in fin.readlines()][offset: offset + length]
        
        logger.info(f'Processing {len(urls)} files')

        queue = None
        
        #file_batches = list(chunks(urls, max(length // (workers * 2), 1)))

        # do it in a single process sequentially
        #for minibatch in file_batches:
        #    process_minibatch(minibatch, tmp_dir, min_length=min_length, queue=queue)
        process_minibatch(urls, tmp_dir, min_length=min_length, queue=queue)

        # do it in parallel with multiple processes
        # with Manager() as manager:
        #     queue = manager.Queue()
        #     monitor_thread = Thread(target=monitor_queue, args=(queue, ))
        #     monitor_thread.start()
                
        #     with ProcessPoolExecutor(max_workers=workers) as executor:
        #         for mini_batch in file_batches:
        #             executor.submit(process_minibatch, mini_batch, tmp_dir, min_length, queue)
                
        #         executor.shutdown(wait=True)
            
        #     queue.put(None)
        #     monitor_thread.join()
    
    logger.info(f'All files processed')


DEBUG = False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert multiple pdfs to markdown.")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes to use")
    parser.add_argument("--min_length", type=int, default=2000, help="Minimum length of pdf to convert")
    args = parser.parse_args()

    if batch_utils.in_batch_cluster():
        g_config = get_config_fromenv()
    else:
        g_config = get_config("conf.yaml")
        commands = [
            f"git clone --branch {g_config.marker_branch} --single-branch {g_config.marker_repo}",
            "marker/inst.sh",
            gen_obsidian_init_command(g_config.branch),
            'python3 marker/convert_batch.py' # use your own script name
        ]

    sas_token = get_blob_sas(g_config.identity_id, g_config.keyvault_name, g_config.secret_name)
    g_config.src_url = g_config.src_url.format(sas_token=sas_token.value)
    g_config.dst_url = g_config.dst_url.format(sas_token=sas_token.value)
    g_config.index_url = g_config.index_url.format(sas_token=sas_token.value)


    if batch_utils.in_batch_cluster():
        # run in cluster
        run(g_config.offset, g_config.length, args.min_length, args.workers)
    else:
        # run locally
 
        if DEBUG:
            # local debug
            logger.info('Running in debug mode')
            run(g_config.offset, g_config.length, args.min_length, args.workers)
        else:
            # submit to cluster
            tot_files = get_file_count(g_config.index_url)
            tasks = []
            current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            job_id = f'{g_config.job_id_prefix}{current_date}'
            for offset in range(0, tot_files, g_config.batch_size):
                task = batch_utils.create_task(
                    job_id,
                    f'task-{offset}-{offset + g_config.batch_size - 1}',
                    ' && '.join(commands),
                    environs={
                        ENV_OFFSET: offset,
                        ENV_LENGTH: g_config.batch_size,
                        ENV_INDEX_URL: g_config.index_url,
                        ENV_SRC_URL: g_config.src_url,
                        ENV_DST_URL: g_config.dst_url,
                        ENV_KEYVAULT_NAME: g_config.keyvault_name,
                        ENV_IDENTITY_ID: g_config.identity_id,
                        ENV_SECRET_NAME: g_config.secret_name
                    }
                )
                tasks.append(task)

                # only submit one batch for debug
                break

            batch_utils.submit_tasks(
                g_config.batch_url,
                g_config.pool_id,
                job_id,
                tasks
            )