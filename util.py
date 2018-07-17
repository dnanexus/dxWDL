#!/usr/bin/env python
from __future__ import print_function
import argparse
from collections import namedtuple
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

AssetDesc = namedtuple('AssetDesc', 'region asset_id project')

max_num_retries = 5
dxWDL_jar_path = "applet_resources/resources/dxWDL.jar"

def get_top_conf_path(top_dir):
    return os.path.join(top_dir, "ref.conf")

def get_crnt_conf_path(top_dir):
    return os.path.join(top_dir, "reference.conf")


def get_project(project_name):
    '''Try to find the project with the given name or id.'''

    # First, see if the project is a project-id.
    try:
        project = dxpy.DXProject(project_name)
        return project
    except dxpy.DXError:
        pass

    project = dxpy.find_projects(name=project_name, name_mode='glob', return_handler=True, level="VIEW")
    project = [p for p in project]
    if len(project) == 0:
        print('Did not find project {0}'.format(project_name), file=sys.stderr)
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name))


def upload_local_file(local_path, project, destFolder, **kwargs):
    for i in range(0,max_num_retries):
        try:
            return dxpy.upload_local_file(filename = local_path,
                                          project = project.get_id(),
                                          folder = destFolder,
                                          show_progress = True,
                                          wait_on_close=True,
                                          **kwargs)
        except:
            print("Sleeping for 5 seconds before trying again")
            time.sleep(5)
    raise Exception("Error uploading file {}".format(local_path))

def make_asset_file(version_id, top_dir):
    asset_spec = {
        "version": version_id,
        "name": "dxWDLrt",
        "title": "dx WDL library",
        "release": "16.04",
        "distribution": "Ubuntu",
        "execDepends": [
            { "name": "openjdk-8-jre-headless" }
        ],
        "instanceType": "mem1_ssd1_x2",
        "description": "Prerequisits for running WDL workflows compiled to the platform"
    }
    with open(os.path.join(top_dir, "applet_resources/dxasset.json"), 'w') as fd:
        fd.write(json.dumps(asset_spec, indent=4))

# build a fat jar file
#
# call sbt-assembly. The tricky part here, is to
# change the working directory to the top of the project.
def sbt_assembly(top_dir):
    crnt_work_dir = os.getcwd()
    os.chdir(os.path.abspath(top_dir))

    # Make sure the directory path exists
    if not os.path.exists("applet_resources"):
        os.mkdir("applet_resources")
    if not os.path.exists("applet_resources/resources"):
        os.mkdir("applet_resources/resources")

    if os.path.exists(dxWDL_jar_path):
        os.remove(dxWDL_jar_path)
    subprocess.check_call(["sbt", "clean"])
    subprocess.check_call(["sbt", "assembly"])
    if not os.path.exists(dxWDL_jar_path):
        raise Exception("sbt assembly failed")
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


def find_asset(project, folder):
    # get asset_id
    assets = list(dxpy.search.find_data_objects(classname="record",
                                                project=project.get_id(),
                                                name="dxWDLrt",
                                                folder=folder,
                                                return_handler=True))
    if len(assets) == 0:
        return None
    if len(assets) == 1:
        return assets[0]
    raise Exception("More than one asset found in folder {}".format(folder))

def build_compiler_jar(version_id, top_dir, project_dict):
    top_conf_path = get_top_conf_path(top_dir)
    crnt_conf_path = get_crnt_conf_path(top_dir)
    with open(top_conf_path, 'r') as fd:
        conf = fd.read()

    # Convert the asset descriptors into ConfigFactory HOCON records.
    # We could use JSON instead, but that would make the file less
    # readable.
    region_project_hocon = []
    all_regions = []
    for region, dx_path in project_dict.iteritems():
        record = "\n".join(["  {",
                            '    region = "{}"'.format(region),
                            '    path = "{}"'.format(dx_path),
                            "  }"])
        region_project_hocon.append(record)
        all_regions.append(region)

    buf = "\n".join(region_project_hocon)
    conf = conf.replace("    region2project = []\n",
                        "    region2project = [\n{}\n]\n".format(buf))

    if os.path.exists(crnt_conf_path):
        os.remove(crnt_conf_path)
    with open(crnt_conf_path, 'w') as fd:
        fd.write(conf)

    # Add the configuration file to the jar archive
    subprocess.check_call(["jar", "uf", dxWDL_jar_path, crnt_conf_path])
    all_regions_str = ", ".join(all_regions)
    print("Added configuration for regions [{}] to jar file".format(all_regions_str))

    # Hygiene, remove the new configuration file, we
    # don't want it to leak into the next build cycle.
    #os.remove(crnt_conf_path)

    # Move the file to the top level directory
    all_in_one_jar = os.path.join(top_dir, "dxWDL-{}.jar".format(version_id))
    shutil.move(os.path.join(top_dir, dxWDL_jar_path),
                all_in_one_jar)
    return all_in_one_jar


def build(project, folder, version_id, top_dir):
    sbt_assembly(top_dir)
    asset = find_asset(project, folder)
    if asset is None:
        make_prerequisits(project, folder, version_id, top_dir)
        asset = find_asset(project, folder)
    region = dxpy.describe(project.get_id())['region']
    return AssetDesc(region, asset.get_id(), project)

# Extract version_id from configuration file
def get_version_id(top_dir):
    pattern = re.compile(r"^(\s*)(version)(\s*)(=)(\s*)(\S+)(\s*)$")
    top_conf_path = get_top_conf_path(top_dir)
    with open(top_conf_path, 'r') as fd:
        for line in fd:
            line_clean = line.replace("\"", "").replace("'", "")
            m = re.match(pattern, line_clean)
            if m is not None:
                return m.group(6).strip()
    raise Exception("version ID not found in {}".format(conf_file))

# Copy an asset across regions
#   path: path to local file
#   dstProj: destination project to copy to. Should be in another region
# return:
#    asset descriptor
def copy_across_regions(local_path, record, dest_region, dest_proj, dest_folder):
    print("copy_across_regions {} {} {} {}:{}".format(local_path,
                                                      record.get_id(),
                                                      dest_region,
                                                      dest_proj.get_id(),
                                                      dest_folder))
    # check if we haven't already created this record, and uploaded the file
    dest_asset = find_asset(dest_proj, dest_folder)
    if dest_asset is not None:
        print("Already copied to region {}".format(dest_region))
        return AssetDesc(dest_region, dest_asset.get_id(), dest_proj)

    # upload
    dest_proj.new_folder(dest_folder, parents=True)
    dxfile = upload_local_file(local_path,
                               dest_proj,
                               dest_folder,
                               hidden=True)
    fid = dxfile.get_id()
    dest_asset = dxpy.new_dxrecord(name=record.name,
                                   types=['AssetBundle'],
                                   details={'archiveFileId': dxpy.dxlink(fid)},
                                   properties=record.get_properties(),
                                   project=dest_proj.get_id(),
                                   folder=dest_folder,
                                   close=True)
    return AssetDesc(dest_region, dest_asset.get_id(), dest_proj)
