#!/usr/bin/env python
from __future__ import print_function
import argparse
import dxpy
import os
import sys
import time
import util

top_dir = os.path.dirname(sys.argv[0])

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
    version_id = util.get_version_id(top_dir)
    print("version: {}".format(version_id))

    util.build(project, folder, version_id, top_dir)


if __name__ == '__main__':
    main()
