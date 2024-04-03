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
chmod +x ./scripts/install/tesseract_5_install.sh
./scripts/install/tesseract_5_install.sh
chmod +x ./scripts/install/ghostscript_install.sh
sudo ./scripts/install/ghostscript_install.sh
cat scripts/install/apt-requirements.txt | xargs sudo apt-get install -y

# 5. Create local.env file
# it's already in repo

# 6. Create conda environment
# no need

# 7. Install python requirements
poetry lock
poetry install

# 8. Update pytorch
poetry remove torch
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# for test
# wget https://arxiv.org/pdf/2304.04487.pdf
# python3 s