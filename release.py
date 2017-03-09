#!/usr/bin/env python
from __future__ import print_function

import argparse
import dxpy
import fnmatch
import json
import pprint
import os
import sys
import subprocess
import time
from shutil import copyfile

top_dir = os.path.dirname(sys.argv[0])
required_libs = ["dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar",
                 "dxWDL.jar"]

def main():
    argparser = argparse.ArgumentParser(description="Create a release for dxWDL")
    argparser.add_argument("--folder", help="Release folder that already exists")
    argparser.add_argument("--project", help="Project where to place release", default="dxWDL")
    args = argparser.parse_args()

    # resolve project
    print("resolving project {}".format(args.project))
    project = dxpy.find_one_project(name = args.project, more_ok=False, return_handler=True)

    # Figure out what the current version is
    version_id = release_version()
    print("version_id={}".format(version_id))

    # Create release folder, if needed
    if args.folder is None:
        folder = time.strftime("/releases/%Y-%m-%d/%H%M%S")
        project.new_folder(folder, parents=True)
        make_prerequisits(project, folder, version_id)
    else:
        folder = args.folder

    print("resolving dxWDL runtime asset")
    asset = dxpy.search.find_one_data_object(classname="record",
                                             project=project.get_id(),
                                             name="dxWDLrt",
                                             folder=folder,
                                             return_handler=True,
                                             more_ok=False)
    print("assetId={}".format(asset.get_id()))

    print("Uploading jar files")
    upload_libs(project, folder)

    print("resolving jar files -- validation ")
    objs = []
    for lib in required_libs:
        objs.append({ "name" : lib, "folder" : folder })
    descs = list(dxpy.search.resolve_data_objects(objs, project=project.get_id()))
    lib_object_ids=[]
    for d in descs:
        print(d[0])
        lib_object_ids.append(d[0]["id"])
    print(lib_object_ids)

    # embed configuration information into dxWDL script
    print("Embedding configuration into dxWDL script")
    script = None
    with open(os.path.join(top_dir,'dxWDL'), 'r') as fd:
        script = fd.read()
    script = script.replace('version_id = None\n',
                            'version_id = "{}"\n'.format(version_id))
    script = script.replace('asset_id = None\n',
                            'asset_id = "{}"\n'.format(asset.get_id()))
    script = script.replace('project_id = None\n',
                            'project_id = "{}"\n'.format(project.get_id()))
    script = script.replace('lib_object_ids = None\n',
                            'lib_object_ids = {}\n'.format(lib_object_ids))
    with open('/tmp/dxWDL', 'w') as fd:
        fd.write(script)

    upload_script(project, folder)

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
    print("Creating a runtime asset from dxWDL")
    destination = project.get_id() + ":" + folder + "/dxWDLrt"
    subprocess.check_call(["dx", "build_asset", "applet_resources",
                           "--destination", destination])
    print("")

def release_version():
    classpath= [
        top_dir + "/lib/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar",
        top_dir + "/applet_resources/resources/dxWDL.jar"
    ]
    version_id = subprocess.check_output(["java", "-cp", ":".join(classpath), "dxWDL.Main", "version"])
    return version_id

def make_asset_file(version_id):
    asset_spec = {
        "version": version_id,
        "name": "dxWDLrt",
        "title": "dx WDL library",
        "release": "14.04",
        "distribution": "Ubuntu",
        "execDepends": [
            { "name": "dx-java-bindings" },
            { "name": "openjdk-8-jre-headless" }
        ],
        "instanceType": "mem1_ssd1_x2",
        "description": "Prerequisits for running WDL workflows compiled to the platform"
    }
    with open("applet_resources/dxasset.json", 'w') as fd:
        fd.write(json.dumps(asset_spec, indent=4))


def upload_libs(project, folder):
    for fname in ["applet_resources/resources/dxWDL.jar",
                  "lib/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar"]:
        dxpy.upload_local_file(filename = os.path.join(top_dir, fname),
                               project = project.get_id(),
                               folder = folder,
                               wait_on_close=True)

def upload_script(project, folder):
    def upload_one_file(local_path, destFolder):
        dxpy.upload_local_file(filename = local_path,
                               project = project.get_id(),
                               folder = destFolder,
                               wait_on_close=True)

    upload_one_file("/tmp/dxWDL", folder)

    # Remove the old script
    old_objs = list(dxpy.search.find_data_objects(
        classname="file",
        project=project.get_id(),
        name="dxWDL_latest",
        folder="/",
        return_handler=True))
    dxpy.api.project_remove_objects(project.get_id(), {"objects": old_objs})

    # Update the latest release script
    copyfile("/tmp/dxWDL", "/tmp/dxWDL_latest")
    upload_one_file("/tmp/dxWDL_latest", "/")


if __name__ == '__main__':
    main()
