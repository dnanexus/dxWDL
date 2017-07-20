#!/usr/bin/env python
from __future__ import print_function
import argparse
import dxpy
import os
import sys
import time
import util

top_dir = os.path.dirname(sys.argv[0])

SUPPORTED_REGIONS = ["aws:us-east-1", "aws:ap-southeast-2"]
PROJECT_DICT = {
    "aws:us-east-1" :  "dxWDL" ,
    "aws:ap-southeast-2" : "dxWDL_Sydney"
}

def main():
    argparser = argparse.ArgumentParser(description="Build the dxWDL jar file")
    argparser.add_argument("--folder",
                           help="Destination folder")
    argparser.add_argument("--project",
                           help="Destination project")
    argparser.add_argument("--multi-region",
                           help="Copy to all supported regions",
                           action='store_true',
                           default=False)
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

    # build the asset
    home_ad = util.build(project, folder, version_id, top_dir)

    ad_all = [home_ad]
    if args.multi_region and len(SUPPORTED_REGIONS) > 1:
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
        for region in SUPPORTED_REGIONS:
            if region != home_ad.region:
                proj = PROJECT_DICT[region]
                if proj is None:
                    raise Exception("No project configured for region {}".format(region))
                dest_proj = util.get_project(proj)
                dest_ad = util.copy_across_regions(rtlib_path, home_rec, region, dest_proj, folder)
                ad_all.append(dest_ad)

    # build the final jar file, containing a list of the per-region
    # assets
    util.construct_conf_file(version_id, top_dir, ad_all)

if __name__ == '__main__':
    main()
