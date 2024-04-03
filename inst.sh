#!/bin/bash
DEBIAN_FRONTEND=noninteractive

# Install required packages
sudo apt-get install -y curl wget git make lsb-release gcc

# 1. Install poetry
curl -sSL https://install.python-poetry.org | python3 -

echo "HOME=$HOME"
pwd

# 2. Add `export PATH="$HOME/.local/bin:$PATH"` to your shell configuration file.
export PATH="$HOME/.local/bin:$PATH"
echo "PATH=$PATH"

# 3. Clone code from Github
cd marker

# 4. Install libraries

# Add the missing public key
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 82F409933771AC78

chmod +x ./scripts/install/tesseract_5_install.sh
./scripts/install/tesseract_5_install.sh
chmod +x ./scripts/install/ghostscript_install.sh
sudo ./scripts/install/ghostscript_install.sh
cat scripts/install/apt-requirements.txt | xargs sudo apt-get install -y

# 5. Create local.env file
echo TESSDATA_PREFIX=/usr/share/tesseract-ocr/5/tessdata > local.env

pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
pip install -q --force-reinstall --no-cache-dir git+https://msrasia.visualstudio.com/DefaultCollection/EBP/_git/DataScale@qinsu/main#subdirectory=obsidian/

# for test
python3 convert_single.py sample.pdf sample.md
