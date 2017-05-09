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
max_num_retries = 5

def main():
    argparser = argparse.ArgumentParser(description="Create a release for dxWDL")
    argparser.add_argument("--folder", help="Release folder that already exists")
    argparser.add_argument("--project", help="Project where to place release", default="dxWDL")
    args = argparser.parse_args()

    # resolve project
    print("resolving project {}".format(args.project))
    project = dxpy.find_one_project(name = args.project, more_ok=False, return_handler=True)

    # Create release folder, if needed
    if args.folder is None:
        folder = time.strftime("/releases/%Y-%m-%d/%H%M%S")
        project.new_folder(folder, parents=True)
        make_prerequisits(project, folder)
        print("Uploading jar files")
        upload_libs(project, folder)
    else:
        folder = args.folder

    # Figure out what the current version is
    version_id = release_version()
    print('version_id="{}"'.format(version_id))

    print("resolving dxWDL runtime asset")
    asset = dxpy.search.find_one_data_object(classname="record",
                                             project=project.get_id(),
                                             name="dxWDLrt",
                                             folder=folder,
                                             return_handler=True,
                                             more_ok=False)
    print("assetId={}".format(asset.get_id()))

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
    rm_silent('/tmp/dxWDL')
    rm_silent('/tmp/dxWDL_latest')
    with open('/tmp/dxWDL', 'w') as fd:
        fd.write(script)
    upload_script(project, folder)

    # Create an asset from the dxWDL jar file and its dependencies,
    # this speeds up applet creation.
def build_runtime_asset(project, folder):
    try:
        print("Creating a runtime asset from dxWDL")
        destination = project.get_id() + ":" + folder + "/dxWDLrt"
        subprocess.check_call(["dx", "build_asset", "applet_resources",
                               "--destination", destination])
        return True
    except:
        return False

def make_prerequisits(project, folder):
    # Run make, to ensure that we have an up-to-date jar file
    #
    # Be careful, so that the make invocation will work even if called from a different
    # directory.
    print("Calling make")
    subprocess.check_call(["make", "-C", top_dir, "all"])
    print("")

    # Create the asset description file
    version_id = release_version()
    make_asset_file(version_id)

    for i in range(0, max_num_retries):
        retval = build_runtime_asset(project, folder)
        if retval:
            return
        print("Sleeping for 5 seconds before trying again")
        time.sleep(5)
    raise Exception("Failed to build the dxWDL runtime asset")

def release_version():
    classpath= [
        top_dir + "/lib/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar",
        top_dir + "/applet_resources/resources/dxWDL.jar"
    ]
    version_id = subprocess.check_output(["java", "-cp", ":".join(classpath), "dxWDL.Main", "version"])
    return version_id.strip()

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


def upload_local_file(local_path, project, destFolder):
    def once():
        try:
            dxpy.upload_local_file(filename = local_path,
                                   project = project.get_id(),
                                   folder = destFolder,
                                   wait_on_close=True)
            return True
        except:
            return False

    for i in range(1,5):
        retval = once()
        if retval is True:
            return
        print("Sleeping for 5 seconds before trying again")
        time.sleep(5)
    raise Exception("Error uploading file {}".format(local_path))


def upload_libs(project, folder):
    for fname in ["applet_resources/resources/dxWDL.jar",
                  "lib/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar"]:
        upload_local_file(filename = os.path.join(top_dir, fname),
                          project = project.get_id(),
                          folder = folder)

def upload_script(project, folder):
    upload_local_file("/tmp/dxWDL", project, folder)

    # Remove the old script
    old_objs = list(dxpy.search.find_data_objects(
        classname="file",
        project=project.get_id(),
        name="dxWDL_latest",
        folder="/",
        return_handler=True))
    for obj in old_objs:
        dxpy.api.project_remove_objects(project.get_id(), {"objects": [obj.get_id()]})

    # Update the latest release script
    copyfile("/tmp/dxWDL", "/tmp/dxWDL_latest")
    upload_local_file("/tmp/dxWDL_latest", project, "/")

# Remove a file, and do not throw any exceptions
def rm_silent(path):
    try:
        os.remove(path)
    except:
        pass


if __name__ == '__main__':
    main()
