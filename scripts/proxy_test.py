#!/usr/bin/env python
import argparse
import copy
import fnmatch
import json
import pprint
import os
import re
import sys
import subprocess
import time
import util

here = os.path.dirname(sys.argv[0])
top_dir = os.path.dirname(os.path.abspath(here))
test_dir = os.path.join(os.path.abspath(top_dir), "test")

# Setup
# - squid installed and running locally
def setup():
    print("Check that we can run squid. Make a copy of the configuration file.")
    try:
        subprocess.check_output(["sudo", "service", "squid", "start"])
        subprocess.check_output(["sudo", "cp", "-f", "/etc/squid/squid.conf", "/etc/squid/squid.conf.bak"])
    except Exception as e:
        print("squid is not installed, run this:")
        print("$ sudo apt-get install squid3")
        exit(1)

def shutdown():
    print("shutdown sequence: stop squid, and copy the original configuration file.")
    subprocess.check_output(["sudo", "service", "squid", "stop"])
    subprocess.check_output(["sudo", "cp", "-f", "/etc/squid/squid.conf.bak", "/etc/squid/squid.conf"])

# Compile a simple workflow.
#
# Note: the HTTP_PROXY environment variable points to the local squid
def compile(project, folder, version_id):
    print("Compiling hello.wdl")
    os.environ["HTTP_PROXY"] = "localhost:3128"
    cmdline = [
        "java", "-jar",
        os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
        "compile",
        os.path.join(test_dir,"basic","hello.wdl"),
        "-folder", folder,
        "-project", project.get_id()
    ]
    print(" ".join(cmdline))
    oid = subprocess.check_output(cmdline).strip()
    return oid.decode("ascii")


def build_dirs(project, version_id):
    base_folder = "/builds/{}".format(version_id)
    folder = base_folder + "/proxy"
    project.new_folder(folder, parents=True)
    return folder

# test 1:
# The squid configuration has a deny-all. The compiler should fail.
def test_deny(project, folder, version_id):
    subprocess.check_output(["sudo", "cp",
                             os.path.join(here, "squid_disallow_all.conf"),
                             "/etc/squid/squid.conf"
                             ])
    subprocess.check_output(["sudo", "service", "squid", "reload"])
    succeeded = True
    try:
        compile(project, folder, version_id)
    except Exception as e:
        succeeded = False
    if succeeded:
        print("Error: requests are blocked, but the compiler can reach the API servers")
        exit(1)
    else:
        print("Correct: when blocking http requests, the compiler can't reach the api servers")

# test 2:
# The squid configuration has an allow-all. The compiler should succeed
def test_allow(project, folder, version_id):
    subprocess.check_output(["sudo", "cp",
                             os.path.join(here, "squid_allow_all.conf"),
                             "/etc/squid/squid.conf"
                             ])
    subprocess.check_output(["sudo", "service", "squid", "reload"])
    succeeded = True
    try:
        compile(project, folder, version_id)
    except Exception as e:
        print(e)
        succeeded = False
    if succeeded:
        print("Correct: when allow http requests through squid, the compiler works")
    else:
        print("Error: requests are allowed, however, the compiler can not reach the API servers")
        exit(1)


######################################################################
## Program entry point
def main():
    global test_locked
    argparser = argparse.ArgumentParser(description="Run WDL proxy tests")
    argparser.add_argument("--do-not-build", help="Do not assemble the dxWDL jar file",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAnexus project ID",
                           default="dxWDL_playground")
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
    args = argparser.parse_args()

    print("top_dir={} test_dir={}".format(top_dir, test_dir))

    version_id = util.get_version_id(top_dir)
    project = util.get_project(args.project)
    if project is None:
        raise RuntimeError("Could not find project {}".format(args.project))
    folder = build_dirs(project, version_id)
    print("project: {} ({})".format(project.name, project.get_id()))
    print("folder: {}".format(folder))

    test_dict = {
        "aws:us-east-1" :  project.name + ":" + folder
    }

    # build the dxWDL jar file, only on us-east-1
    if not args.do_not_build:
        util.build(project, folder, version_id, top_dir, test_dict)

    # make sure that squid is installed and running
    setup()

    try:
        test_deny(project, folder, version_id)
        test_allow(project, folder, version_id)
    finally:
        print("Test complete")

    # stop squid
    shutdown()

if __name__ == '__main__':
    main()
