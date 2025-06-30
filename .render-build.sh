#!/usr/bin/env bash
set -e

# Upgrade pip + tools
pip install --upgrade pip setuptools wheel

# Install pandas via binary wheel only (avoid Meson/Cython build)
pip install --only-binary :all: pandas==2.2.2

# Install remaining dependencies
pip install -r requirements.txt
