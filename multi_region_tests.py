#!/usr/bin/env python
from __future__ import print_function

import argparse
import dxpy
import json
import pprint
import os
import sys
import subprocess
import util
from dxpy.exceptions import DXJobFailureError

######################################################################
# multi-region test.
#
# Compile the trivial workflow on all supported regions, and see that it runs.

projects = ["dxWDL_playground", "dxWDL_Sydney", "dxWDL_Azure"]
top_dir = os.path.dirname(sys.argv[0])
test_dir = os.path.join(os.path.abspath(top_dir), "test")
target_folder = "/release_test"

# Build a workflow.
#
# wf             workflow name
# classpath      java classpath needed for running compilation
# folder         destination folder on the platform
def build_test(source_file, proj_name, folder, version_id):
    dx_proj = util.get_project(proj_name)
    if dx_proj is None:
        raise RuntimeError("Could not find project {}".format(proj_name))
    dx_proj.new_folder(folder, parents=True)
    print("Compiling {} to project {}:/{}".format(source_file, proj_name, folder))
    cmdline = [ "java", "-jar",
                os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                "compile",
                source_file,
                "-force",
                "-folder", folder,
                "-project", proj_name ]
    print(" ".join(cmdline))
    oid = subprocess.check_output(cmdline).strip()
    return oid

def main():
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    args = argparser.parse_args()

    version_id = util.get_version_id(top_dir)
    wdl_source_file = os.path.join(test_dir, "basic/trivial.wdl")
    dx_objects = []
    for proj_name in projects:
        oid = build_test(wdl_source_file, proj_name, target_folder, version_id)
        dx_objects.append(oid)

    #oid = build_test(tname, project, applet_folder, version_id, c_flags)

if __name__ == '__main__':
    main()
