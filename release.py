#!/usr/bin/env python
from __future__ import print_function

import argparse
import dxpy
import json
import pprint
import os
import sys
import time
import util

top_dir = os.path.dirname(sys.argv[0])

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
    else:
        folder = args.folder
    print("folder: {}".format(folder))

    # Figure out what the current version is
    version_id = util.get_version_id(top_dir)
    print("version: {}".format(version_id))

    compiler_jar_file = util.build(project, folder, version_id, top_dir)

    # Upload compiler jar file
    util.upload_local_file(compiler_jar_file, project, folder)

if __name__ == '__main__':
    main()
