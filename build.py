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

top_dir = os.path.dirname(sys.argv[0])
conf_file = top_dir + "/src/main/resources/reference.conf"
max_num_retries = 5

def make_asset_file(version_id):
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


def make_prerequisits(project, folder, version_id):
    # Run make, to ensure that we have an up-to-date jar file
    #
    # Be careful, so that the make invocation will work even if called from a different
    # directory.
    print("Calling make")
    subprocess.check_call(["make", "-C", top_dir, "all"])
    print("")

    # Create the asset description file
    make_asset_file(version_id)

    # Create an asset from the dxWDL jar file and its dependencies,
    # this speeds up applet creation.
    for i in range(0, max_num_retries):
        try:
            print("Creating a runtime asset from dxWDL")
            destination = project.get_id() + ":" + folder + "/dxWDLrt"
            subprocess.check_call(["dx", "build_asset", "applet_resources",
                                   "--destination", destination])
            return
        except:
            print("Sleeping for 5 seconds before trying again")
            time.sleep(5)
    raise Exception("Failed to build the dxWDL runtime asset")

# Replace a regex in a file. Be careful to create a new file, and then
# move it to the original place.
def replace_pattern_in_file(source_file_path, pattern, subline):
    fh, target_file_path = mkstemp()
    with open(target_file_path, 'w') as target_file:
        with open(source_file_path, 'r') as source_file:
            for line in source_file:
                if re.match(pattern, line):
                    target_file.write(subline)
                else:
                    target_file.write(line)
    #os.remove(source_file_path)
    shutil.move(target_file_path, source_file_path)


def build(project, folder, version_id):
    make_prerequisits(project, folder, version_id)

    # get asset_id
    asset = dxpy.search.find_one_data_object(classname="record",
                                             project=project.get_id(),
                                             name="dxWDLrt",
                                             folder=folder,
                                             return_handler=True,
                                             more_ok=False)
    print("assetId={}".format(asset.get_id()))

    # update asset_id in configuration file
    replace_pattern_in_file(conf_file,
                            "^(\s*)(asset_id)(.*)",
                            "    asset_id = \"{}\"\n".format(asset.get_id()))

    # sbt assembly
    subprocess.check_call(["make", "-C", top_dir, "all"])

    # Move the file to the top level directory
    all_in_one_jar = os.path.join(top_dir, "dxWDL-{}.jar".format(version_id))
    shutil.move(os.path.join(top_dir, "applet_resources/resources/dxWDL.jar"),
                all_in_one_jar)
    return all_in_one_jar


# Extract version_id from configuration file
def get_version_id():
    pattern = re.compile(r"^(\s*)(version)(\s*)(=)(\s*)(\S+)(\s*)$")
    with open(conf_file, 'r') as fd:
        for line in fd:
            line_clean = line.replace("\"", "").replace("'", "")
            m = re.match(pattern, line_clean)
            if m is not None:
                return m.group(6).strip()
    raise Exception("version ID not found in {}".format(conf_file))

def main():
    argparser = argparse.ArgumentParser(description="Build dxWDL jar file")
    argparser.add_argument("--folder", help="Destination folder")
    argparser.add_argument("--project", help="Destination project")
    args = argparser.parse_args()

    # resolve project
    if args.project is None:
        project = dxpy.DXProject(os.environ['DX_PROJECT_CONTEXT_ID'])
    else:
        project = dxpy.find_one_project(name = args.project, more_ok=False, return_handler=True)
    print("project: {} ({})".format(project.name, project.get_id()))

    # Create release folder, if needed
    if args.folder is None:
        folder = time.strftime("/builds/%Y-%m-%d/%H%M%S")
        project.new_folder(folder, parents=True)
    else:
        folder = args.folder
    print("folder: {}".format(folder))

    # Figure out what the current version is
    version_id = get_version_id()
    print("version: {}".format(version_id))

    build(project, folder, version_id)


if __name__ == '__main__':
    main()
