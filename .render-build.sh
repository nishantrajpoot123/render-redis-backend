#!/usr/bin/env bash
pip install --upgrade pip setuptools wheel
pip install --only-binary :all: pandas==2.2.2
pip install -r requirements.txt
