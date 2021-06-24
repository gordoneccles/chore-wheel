#! /bin/bash

rm -rf lambda.zip
cd /Users/gordoneccles/.pyenv/versions/chore-wheel-venv/lib/python3.9/site-packages
zip -r9 ${OLDPWD}/lambda.zip .
cd $OLDPWD
zip -g lambda.zip chore_wheel.py
