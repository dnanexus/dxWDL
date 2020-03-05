#!/usr/bin/env python
import argparse
from collections import namedtuple
import dxpy
import json
import multiprocessing
import pprint
import os
import subprocess

from typing import Callable, Iterator, Union, Optional, List
import time


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


def err_exit(msg):
    print(msg)
    exit(1)

# Check if a program (wget, curl, etc.) is on the path, and
# can be called.
def _which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file
    return None

def download_and_calc_md5sum(url):
    print("downloading {} and calculating checksum".format(url))
    dest_filename = "/tmp/A"
    if os.path.exists(dest_filename):
        os.remove(dest_filename)

    # Check if aria2 present
    # Use that instead of wget
    aria2c_exe = _which("aria2c")
    if aria2c_exe is None:
        wget_exe = _which("wget")
        if wget_exe is None:
            err_exit("wget is not installed on this system")

        cmd = ["wget", "--tries=5", "--quiet"]
        cmd += ["-O", dest_filename, url]
    else:
        print("aria2c found in path so using that instead of wget \n")
        # aria2c does not allow more than 16 connections per server
        max_connections = min(16, multiprocessing.cpu_count())
        cmd = ["aria2c", "--check-certificate=false", "-s", str(max_connections), "-x", str(max_connections)]
        # Split path properly for aria2c
        # If '-d' arg not provided, aria2c uses current working directory
        cwd = os.getcwd()
        directory, filename = os.path.split(dest_filename)
        directory = cwd if directory in ["", cwd] else directory
        cmd += ["-o", filename, "-d", os.path.abspath(directory), url]

    try:
        if aria2c_exe is not None:
            print("Downloading symbolic link with aria2c")
        else:
            print("Downloading symbolic link with wget")
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        msg = ""
        if e and e.output:
            msg = e.output.strip()
        err_exit("Failed to call download: {cmd}\n{msg}\n".format(cmd=str(cmd), msg=msg))

    result = subprocess.run(["md5sum", "-b", dest_filename], stdout=subprocess.PIPE)
    output=result.stdout.decode('utf-8')
    os.remove(dest_filename)
    return output.split()[0]

def main():
    argparser = argparse.ArgumentParser(description="Create symbolic link")
    argparser.add_argument("--project", help="DNAnexus project")
    argparser.add_argument("--folder", help="folder in project")
    argparser.add_argument("--url", help="the url to reference")

    args = argparser.parse_args()
    if args.project is None:
        print("Must provide project")
        exit(1)
    if args.url is None:
        print("Must provide url")
        exit(1)

    name = os.path.basename(args.url)
    dx_proj = get_project(args.project)

    folder = "/"
    if args.folder is not None:
        folder = args.folder

    md5sum = download_and_calc_md5sum(args.url)

    # create a symlink on the platform, with the correct checksum
    input_params = {
        'name' : name,
        'project': dx_proj.get_id(),
        'drive': "drive-PUBLISHED",
        'md5sum': md5sum,
        'symlinkPath': {
            'object': args.url
        },
        'folder' : folder
    }

    result = dxpy.api.file_new(input_params=input_params)
    f = dxpy.DXFile(dxid = result["id"], project = dx_proj.get_id())

    desc = f.describe()
    print(desc)

if __name__ == '__main__':
    main()
