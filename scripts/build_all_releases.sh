#!/bin/bash


dx login --token $STAGING_TOKEN
./scripts/build_release.py --force --multi-region

dx login --token $PRODUCTION_TOKEN
./scripts/build_release.py --force --multi-region
