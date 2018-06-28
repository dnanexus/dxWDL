#!/usr/bin/env python
from __future__ import print_function
import argparse
import dxpy
import os
import sys
import time
import util

top_dir = os.path.dirname(sys.argv[0])

HOME_REGION = "aws:us-east-1"

# To add region R, create a project for it, dxWDL_R, and add
# a mapping to the lists
#    R : dxWDL_R
TEST_DICT = {
    "aws:us-east-1" :  "dxWDL_playground",
    "aws:ap-southeast-2" : "dxWDL_Sydney",
    "azure:westus" : "dxWDL_Azure"
}
RELEASE_DICT = {
    "aws:us-east-1" :  "dxWDL",
    "aws:ap-southeast-2" : "dxWDL_Sydney",
    "azure:westus" : "dxWDL_Azure"
}

def main():
    argparser = argparse.ArgumentParser(description="Build the dxWDL jar file")
    argparser.add_argument("--folder",
                           help="Destination folder")
    argparser.add_argument("--multi-region",
                           help="Copy to all supported regions",
                           action='store_true',
                           default=False)
    argparser.add_argument("--release",
                           help="Create a dxWDL release, implies multi-region",
                           action='store_true',
                           default=False)
    args = argparser.parse_args()

    # Choose which dictionary to use
    if args.release:
        project_dict = RELEASE_DICT
    else:
        project_dict = TEST_DICT
    project = util.get_project(project_dict[HOME_REGION])
    print("project: {} ({})".format(project.name, project.get_id()))


    # Figure out what the current version is
    version_id = util.get_version_id(top_dir)
    print("version: {}".format(version_id))

    # Set the folder, build one if necessary
    if args.folder is not None:
        folder = args.folder
    elif args.release:
        folder = time.strftime("/releases/%Y-%m-%d/%H%M%S")
        project.new_folder(folder, parents=True)
    else:
        folder = time.strftime("/builds/%Y-%m-%d/%H%M%S")
        project.new_folder(folder, parents=True)
    print("folder: {}".format(folder))

    # build multi-region jar for releases, or
    # if explicitly specified
    multi_region = args.multi_region
    if args.release:
        multi_region = True

    # build the asset
    home_ad = util.build(project, folder, version_id, top_dir)

    # build the compiler jar file
    # projects
    jar_path = util.build_compiler_jar(version_id, top_dir, project_dict)

    if multi_region:
        # download dxWDL runtime library
        home_rec = dxpy.DXRecord(home_ad.asset_id)
        fid = home_rec.get_details()['archiveFileId']['$dnanexus_link']
        fn = dxpy.describe(fid)['name']
        rtlib_path = "/tmp/{}".format(fn)
        print("Download asset file {}".format(fn))
        dxpy.download_dxfile(fid,
                             rtlib_path,
                             show_progress=True)

        # copy to all other regions
        for region in project_dict.keys():
            if region != home_ad.region:
                proj = project_dict[region]
                if proj is None:
                    raise Exception("No project configured for region {}".format(region))
                dest_proj = util.get_project(proj)
                if dest_proj is not None:
                    dest_ad = util.copy_across_regions(rtlib_path, home_rec, region, dest_proj, folder)
                else:
                    print("No project named {}".format(proj))

    # Upload compiler jar file
    if args.release:
        util.upload_local_file(jar_path, project, folder)

if __name__ == '__main__':
    main()
