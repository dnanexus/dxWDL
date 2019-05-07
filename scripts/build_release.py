#!/usr/bin/env python
from __future__ import print_function
import argparse
import dxpy
import os
import subprocess
import sys
import time
import util

here = os.path.dirname(sys.argv[0])
top_dir = os.path.dirname(os.path.abspath(here))

HOME_REGION = "aws:us-east-1"

# To add region R, create a project for it, dxWDL_R, and add
# a mapping to the lists
#    R : dxWDL_R
RELEASE_DICT = {
    "aws:us-east-1" :  "dxWDL",
    "aws:ap-southeast-2" : "dxWDL_Sydney",
    "azure:westus" : "dxWDL_Azure",
    "azure:westeurope" : "dxWDL_Amsterdam",
    "aws:eu-central-1" : "dxWDL_Berlin"
}

def main():
    argparser = argparse.ArgumentParser(description="Build a dxWDL release")
    argparser.add_argument("--force",
                           help="Build even if the there is an existing version",
                           action='store_true',
                           default=False)
    argparser.add_argument("--multi-region",
                           help="Copy to all supported regions",
                           action='store_true',
                           default=False)
    args = argparser.parse_args()

    # Choose which dictionary to use
    project_dict = RELEASE_DICT
    project = util.get_project(project_dict[HOME_REGION])
    print("project: {} ({})".format(project.name, project.get_id()))

    # Figure out what the current version is
    version_id = util.get_version_id(top_dir)
    print("version: {}".format(version_id))

    # Set the folder
    folder = "/releases/{}".format(version_id)
    print("folder: {}".format(folder))

    # remove the existing directory paths
    if args.force:
        for proj_name in project_dict.values():
            print("removing path {}:{}".format(proj_name, folder))
            dx_proj = util.get_project(proj_name)
            try:
                dx_proj.remove_folder(folder, recurse=True)
            except dxpy.DXError:
                pass

    # Make sure the target directory exists
    project.new_folder(folder, parents=True)

    # build multi-region jar for releases, or
    # if explicitly specified
    multi_region = args.multi_region

    # Build the asset, and the compiler jar file.
    path_dict = dict(map(lambda kv: (kv[0], kv[1] + ":" + folder),
                         project_dict.items()))
    (jar_path, home_ad) = util.build(project, folder, version_id, top_dir, path_dict)

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
    util.upload_local_file(jar_path, project, folder)

if __name__ == '__main__':
    main()
