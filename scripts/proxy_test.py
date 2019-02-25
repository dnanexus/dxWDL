#!/usr/bin/env python
import argparse
import copy
import fnmatch
import getpass
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

# Use ip-tables to block this user from accessing https addresses
def iptables_block_user():
    this_user = getpass.getuser()
    subprocess.check_output(["sudo", "iptables", "--new-chain", "chk_user"])
    subprocess.check_output(["sudo", "iptables", "-A", "OUTPUT",
                             "-m", "owner", "--uid-owner", this_user,
                             "-j", "chk_user"])
    subprocess.check_output(["sudo", "iptables", "-A", "chk_user",
                             "-p", "tcp", "--dport", "443",
                             "-j", "REJECT"])

# delete the new ip-table rule
def iptables_clean():
    subprocess.check_output(["sudo", "iptables", "-F"])
    subprocess.check_output(["sudo", "iptables", "-X"])

# Setup
# - squid installed and running locally
def setup():
    print("Clean the iptables state")
    iptables_clean()
    print("Check that we can run squid. Make a copy of the configuration file.")
    try:
        subprocess.check_output(["sudo", "service", "squid", "start"])
        subprocess.check_output(["sudo", "cp", "-f", "/etc/squid/squid.conf", "/etc/squid/squid.conf.bak"])
    except Exception as e:
        print("squid is not installed, run this:")
        print("$ sudo apt-get install squid3")
        exit(1)

def shutdown():
    iptables_clean()
    print("shutdown sequence: stop squid, and copy the original configuration file.")
    subprocess.check_output(["sudo", "service", "squid", "stop"])
    subprocess.check_output(["sudo", "cp", "-f", "/etc/squid/squid.conf.bak", "/etc/squid/squid.conf"])

# Compile a simple workflow.
#
# Note: the HTTP_PROXY environment variable points to the local squid
def compile(project, folder, version_id, with_proxy):
    print("Compiling hello.wdl")
    if with_proxy:
        os.environ["HTTP_PROXY"] = "localhost:3128"
    cmdline = [
        "java", "-jar",
        os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
        "compile",
        os.path.join(test_dir,"basic","hello.wdl"),
        "-force",
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
        compile(project, folder, version_id, True)
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
        compile(project, folder, version_id, True)
    except Exception as e:
        print(e)
        succeeded = False
    if succeeded:
        print("Correct: http requests are passing through the proxy, reaching the apiserver")
    else:
        print("Error: requests are allowed through the proxy, however, the API servers are unreachable")
        exit(1)

# test 3:
# block https access for this user, the compiler should fail.
def test_network_blocking(project, folder, version_id):
    iptables_block_user()
    succeeded = True
    try:
        compile(project, folder, version_id, False)
    except Exception as e:
        succeeded = False
    iptables_clean()

    if succeeded:
        print("Error: network blocking with ip-tables failed. Something is wrong with ip-tables,"
              " it can't stop this user from accessing https URLs")
        exit(1)
    else:
        print("Correct: user can be blocked from accessing the network")


# test 3:
# set squid as a proxy,
# block https access for this user.
# This is supposed to succeed.
def test_squid_allows_bypassing_firewall(project, folder, version_id):
    # run squid
    subprocess.check_output(["sudo", "cp",
                             os.path.join(here, "squid_allow_all.conf"),
                             "/etc/squid/squid.conf"
                             ])
    subprocess.check_output(["sudo", "service", "squid", "reload"])

    # Use ip-tables to block this user from accessing https addresses
    iptables_block_user()
    succeeded = True
    try:
        compile(project, folder, version_id, True)
    except Exception as e:
        print(e)
        succeeded = False
    # delete the new ip-table rule
    iptables_clean()

    if succeeded:
        print("Correct: setting a proxy allows network access, when"
              " direct access is blocked")
    else:
        print("Error: compiler was not using the proxy, it was sneaking behind it")
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

    # make sure that squid is installed and running
    setup()

    try:
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

#        test_deny(project, folder, version_id)
#        test_allow(project, folder, version_id)
        test_network_blocking(project, folder, version_id)
        test_squid_allows_bypassing_firewall(project, folder, version_id)
    finally:
        print("Test complete")
        shutdown()

if __name__ == '__main__':
    main()
