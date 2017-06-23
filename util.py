#!/usr/bin/env python
from __future__ import print_function
import argparse
import dxpy
import fnmatch
import json
import pprint
import os
import re
import shutil
import subprocess
import sys
from tempfile import mkstemp
import time

max_num_retries = 5

def get_top_conf_file(top_dir):
    return os.path.join(top_dir, "reference.conf")

def get_crnt_conf_file(top_dir):
    try:
        os.mkdir(os.path.join(top_dir, "src/main/resources"))
    except:
        pass
    return os.path.join(top_dir, "src/main/resources/reference.conf")


def make_asset_file(version_id, top_dir):
    asset_spec = {
        "version": version_id,
        "name": "dxWDLrt",
        "title": "dx WDL library",
        "release": "14.04",
        "distribution": "Ubuntu",
        "execDepends": [
            { "name": "openjdk-8-jre-headless" }
        ],
        "instanceType": "mem1_ssd1_x2",
        "description": "Prerequisits for running WDL workflows compiled to the platform"
    }
    with open(os.path.join(top_dir, "applet_resources/dxasset.json"), 'w') as fd:
        fd.write(json.dumps(asset_spec, indent=4))

# call sbt-assembly. The tricky part here, is to
# change the working directory to the top of the project.
def sbt_assembly(top_dir):
    crnt_work_dir = os.getcwd()
    os.chdir(os.path.abspath(top_dir))
    subprocess.check_call(["sbt", "assembly"])
    os.chdir(crnt_work_dir)

# Build a dx-asset from the runtime library.
# Go to the top level directory, before running "dx"
def build_asset(top_dir, destination):
    crnt_work_dir = os.getcwd()
    os.chdir(os.path.abspath(top_dir))
    subprocess.check_call(["dx", "build_asset", "applet_resources",
                           "--destination", destination])
    os.chdir(crnt_work_dir)


def make_prerequisits(project, folder, version_id, top_dir):
    # build a fat jar file
    sbt_assembly(top_dir)

    # Create the asset description file
    make_asset_file(version_id, top_dir)

    # Create an asset from the dxWDL jar file and its dependencies,
    # this speeds up applet creation.
    for i in range(0, max_num_retries):
        try:
            print("Creating a runtime asset from dxWDL")
            destination = project.get_id() + ":" + folder + "/dxWDLrt"
            build_asset(top_dir, destination)
            return
        except:
            print("Sleeping for 5 seconds before trying again")
            time.sleep(5)
    raise Exception("Failed to build the dxWDL runtime asset")


def build(project, folder, version_id, top_dir):
    make_prerequisits(project, folder, version_id, top_dir)

    # get asset_id
    asset = dxpy.search.find_one_data_object(classname="record",
                                             project=project.get_id(),
                                             name="dxWDLrt",
                                             folder=folder,
                                             return_handler=True,
                                             more_ok=False)
    print("assetId={}".format(asset.get_id()))

    # update asset_id in configuration file
    top_conf_file = get_top_conf_file(top_dir)
    crnt_conf_file = get_crnt_conf_file(top_dir)
    conf = None
    with open(top_conf_file, 'r') as fd:
        conf = fd.read()
    conf = conf.replace('    asset_id = None\n',
                        '    asset_id = "{}"\n'.format(asset.get_id()))
    with open(crnt_conf_file, 'w') as fd:
        fd.write(conf)

    # build a fat jar file
    sbt_assembly(top_dir)

    # Move the file to the top level directory
    all_in_one_jar = os.path.join(top_dir, "dxWDL-{}.jar".format(version_id))
    shutil.move(os.path.join(top_dir, "applet_resources/resources/dxWDL.jar"),
                all_in_one_jar)
    return all_in_one_jar


# Extract version_id from configuration file
def get_version_id(top_dir):
    pattern = re.compile(r"^(\s*)(version)(\s*)(=)(\s*)(\S+)(\s*)$")
    top_conf_file = get_top_conf_file(top_dir)
    with open(top_conf_file, 'r') as fd:
        for line in fd:
            line_clean = line.replace("\"", "").replace("'", "")
            m = re.match(pattern, line_clean)
            if m is not None:
                return m.group(6).strip()
    raise Exception("version ID not found in {}".format(conf_file))

def upload_local_file(local_path, project, destFolder):
    for i in range(0,max_num_retries):
        try:
            dxpy.upload_local_file(filename = local_path,
                                   project = project.get_id(),
                                   folder = destFolder,
                                   wait_on_close=True)
            return
        except:
            print("Sleeping for 5 seconds before trying again")
            time.sleep(5)
    raise Exception("Error uploading file {}".format(local_path))
