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

#dxda_version = "v0.2.2"
dxda_version = "20190909212832_c28a2ad"
dxfuse_version = "v0.15"
max_num_retries = 5

def dxWDL_jar_path(top_dir):
    return os.path.join(top_dir, "applet_resources/resources/dxWDL.jar")

def get_appl_conf_path(top_dir):
    return os.path.join(top_dir, "src", "main", "resources", "application.conf")

def get_runtime_conf_path(top_dir):
    return os.path.join(top_dir, "src", "main", "resources", "dxWDL_runtime.conf")


def get_project(project_name):
    '''Try to find the project with the given name or id.'''

    # First, see if the project is a project-id.
    try:
        project = dxpy.DXProject(project_name)
        return project
    except dxpy.DXError:
        pass

    project = dxpy.find_projects(name=project_name, return_handler=True, level="VIEW")
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
            { "name": "openjdk-8-jre-headless" },
            { "name": "bzip2" },
            { "name": "jq" }
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
def _sbt_assembly(top_dir, version_id):
    os.chdir(os.path.abspath(top_dir))

    # Make sure the directory path exists
    if not os.path.exists("applet_resources"):
        os.mkdir("applet_resources")
    if not os.path.exists("applet_resources/resources"):
        os.mkdir("applet_resources/resources")

    jar_path = dxWDL_jar_path(top_dir)
    if os.path.exists(jar_path):
        os.remove(jar_path)
    subprocess.check_call(["sbt", "clean"])
    subprocess.check_call(["sbt", "assembly"])
    if not os.path.exists(jar_path):
        raise Exception("sbt assembly failed")
    return jar_path

# download dxda for linux, and place it in the resources
# sub-directory.
def _download_dxda_into_resources(top_dir):
    os.chdir(os.path.join(top_dir, "applet_resources"))

    # make sure the resources directory exists
    if not os.path.exists("resources/usr/bin"):
        os.makedirs("resources/usr/bin")

    # download dxda release, and place it in the resources directory
    trg_dxda_tar = "resources/dx-download-agent-linux.tar"
    if dxda_version.startswith("v"):
        # A proper download-agent release, it starts with a "v"
        subprocess.check_call([
            "wget",
            "https://github.com/dnanexus/dxda/releases/download/{}/dx-download-agent-linux.tar".format(dxda_version),
            "-O",
            trg_dxda_tar])
    else:
        # A snapshot of the download-agent development branch
        command = """sudo  docker run --rm --entrypoint=\'\' dnanexus/dxda:{} cat /builds/dx-download-agent-linux.tar > {}""".format(dxda_version, trg_dxda_tar)
        p = subprocess.Popen(command, universal_newlines=True, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        text = p.stdout.read()
        retcode = p.wait()
        print("downloading dxda{} {}", retcode, text)

    subprocess.check_call(["tar", "-C", "resources", "-xvf", trg_dxda_tar])
    os.rename("resources/dx-download-agent-linux/dx-download-agent",
              "resources/usr/bin/dx-download-agent")
    os.chmod("resources/usr/bin/dx-download-agent", 0o775)
    os.remove(trg_dxda_tar)
    shutil.rmtree("resources/dx-download-agent-linux")

def _add_dxfuse_to_resources(top_dir):
    # make sure the resources directory exists
    os.chdir(os.path.join(top_dir, "applet_resources"))
    if not os.path.exists("resources/usr/bin"):
        os.makedirs("resources/usr/bin")
    subprocess.check_call([
        "wget",
        "https://github.com/dnanexus/dxfuse/releases/download/{}/dxfuse-linux".format(dxfuse_version),
        "-O",
        os.path.join("resources/usr/bin/dxfuse") ])
    os.chmod("resources/usr/bin/dxfuse", 0o775)

# Build a dx-asset from the runtime library.
# Go to the top level directory, before running "dx"
def build_asset(top_dir, destination):
    crnt_work_dir = os.getcwd()

    # build the platform asset
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

# Create a dxWDL.conf file in the top level directory. It
# holds a mapping from region to project, where the runtime
# asset is stored.
def _gen_config_file(version_id, top_dir, project_dict):
    # Create a record for each region
    region_project_hocon = []
    all_regions = []
    for region, dx_path in project_dict.items():
        record = "\n".join(["  {",
                            '    region = "{}"'.format(region),
                            '    path = "{}"'.format(dx_path),
                            "  }"])
        region_project_hocon.append(record)
        all_regions.append(region)

    buf = "\n".join(region_project_hocon)
    conf = "\n".join(["dxWDL {",
                      "    region2project = [\n{}\n]".format(buf),
                      "}"])

    rt_conf_path = get_runtime_conf_path(top_dir)
    if os.path.exists(rt_conf_path):
        os.remove(rt_conf_path)
    with open(rt_conf_path, 'w') as fd:
        fd.write(conf)
    all_regions_str = ", ".join(all_regions)
    print("Built configuration regions [{}] into {}".format(all_regions_str,
                                                            rt_conf_path))

def build(project, folder, version_id, top_dir, path_dict):
    asset = find_asset(project, folder)
    if asset is None:
        # get a copy of the dxfuse executable
        _add_dxfuse_to_resources(top_dir)

        # Create a configuration file
        _gen_config_file(version_id, top_dir, path_dict)
        jar_path = _sbt_assembly(top_dir, version_id)

        # get a copy of the download agent (dxda)
        _download_dxda_into_resources(top_dir)

        make_prerequisits(project, folder, version_id, top_dir)
        asset = find_asset(project, folder)

        # Move the file to the top level directory
        all_in_one_jar = os.path.join(top_dir, "dxWDL-{}.jar".format(version_id))
        shutil.move(os.path.join(top_dir, jar_path),
                    all_in_one_jar)

    region = dxpy.describe(project.get_id())['region']
    ad = AssetDesc(region, asset.get_id(), project)

    # Hygiene, remove the new configuration file, we
    # don't want it to leak into the next build cycle.
    # os.remove(crnt_conf_path)
    return ad


# Extract version_id from configuration file
def get_version_id(top_dir):
    pattern = re.compile(r"^(\s*)(version)(\s*)(=)(\s*)(\S+)(\s*)$")
    appl_conf_path = get_appl_conf_path(top_dir)
    with open(appl_conf_path, 'r') as fd:
        for line in fd:
            line_clean = line.replace("\"", "").replace("'", "")
            m = re.match(pattern, line_clean)
            if m is not None:
                return m.group(6).strip()
    raise Exception("version ID not found in {}".format(conf_file))
