#!/usr/bin/env python
from __future__ import print_function

import argparse
import dxpy
import json
import pprint
import os
import time
import sys
import subprocess
import util
from dxpy.exceptions import DXJobFailureError

######################################################################
# multi-region test.
#
# Compile the trivial workflow on all supported regions, and see that it runs.

here = os.path.dirname(sys.argv[0])
top_dir = os.path.dirname(os.path.abspath(here))
test_dir = os.path.join(os.path.abspath(top_dir), "test")

projects = ["dxWDL_playground", "dxWDL_Sydney", "dxWDL_Azure"]
target_folder = "/release_test"

def wait_for_completion(test_exec_objs):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for exec_obj in test_exec_objs:
            exec_obj.wait_on_done()
    finally:
        noise.kill()
    print("done")

# Run [workflow] on several inputs, return the analysis ID.
def run_workflow(dx_proj, test_folder, oid):
    def once():
        try:
            dx_proj.new_folder(test_folder, parents=True)
            wf = dxpy.DXWorkflow(project=dx_proj.get_id(), dxid=oid)
            return wf.run({},
                          project=dx_proj.get_id(),
                          folder=test_folder)
        except Exception, e:
            print("exception message={}".format(e))
            return None

    for i in range(1,5):
        retval = once()
        if retval is not None:
            return retval
        print("Sleeping for 5 seconds before trying again")
        time.sleep(5)
    raise RuntimeError("running workflow")

# Build a workflow.
#
# wf             workflow name
# classpath      java classpath needed for running compilation
# folder         destination folder on the platform
def build_test(source_file, dx_proj, folder, version_id):
    dx_proj.new_folder(folder, parents=True)
    print("Compiling {} to project {}:/{}".format(source_file, dx_proj.name, folder))
    cmdline = [ "java", "-jar",
                os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                "compile",
                source_file,
                "-force",
                "-locked",
                "-folder", folder,
                "-project", dx_proj.get_id() ]
    print(" ".join(cmdline))
    oid = subprocess.check_output(cmdline).strip()
    return oid

def main():
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    argparser.add_argument("--compile-only", help="Only compile the workflows, don't run them",
                           action="store_true", default=False)
    args = argparser.parse_args()

    version_id = util.get_version_id(top_dir)
    wdl_source_file = os.path.join(test_dir, "basic/trivial.wdl")
    dx_objects = []
    test_exec_objs=[]

    # build version of the applet on all regions
    for proj_name in projects:
        dx_proj = util.get_project(proj_name)
        if dx_proj is None:
            raise RuntimeError("Could not find project {}".format(proj_name))
        oid = build_test(wdl_source_file, dx_proj, target_folder, version_id)
        if args.compile_only:
            continue
        anl = run_workflow(dx_proj, target_folder, oid)
        print("Running {}".format(oid))
        test_exec_objs.append(anl)

    # Wait for completion
    print("executables: " + ", ".join([a.get_id() for a in test_exec_objs]))
    wait_for_completion(test_exec_objs)

if __name__ == '__main__':
    main()
