#!/usr/bin/env python

import argparse
import dxpy
import json
import os
import subprocess
import time
import sys

import util

here = os.path.dirname(sys.argv[0])
top_dir = os.path.dirname(os.path.abspath(here))

HOME_REGION = "aws:us-east-1"
URL_DURATION = 60 * 60 * 24
SLEEP_TIME = 5
COPY_FILE_APP_NAME = "dxwdl_copy_file"
COPY_FILE_APP = dxpy.find_one_app(zero_ok=False, more_ok=False, name=COPY_FILE_APP_NAME, return_handler=True)
NUM_RETRIES = 1

TEST_DICT = {
    "aws:us-east-1" :  "dxWDL_playground"
}

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

# 1. Use the clone-asset app to copy the file into [region].
# 2. Create a record pointing to the hidden file in [region].
def _clone_asset_into_region(region, dest_proj_id, asset_file_name, dest_folder, url):
    """
    Clone file into a remote region.
    """
    dxjob = COPY_FILE_APP.run(app_input = { "url" : url,
                                            "folder" : dest_folder,
                                            "filename" : asset_file_name },
                              name = "copy to region {}".format(region),
                              project = dest_proj_id)
    print('{region}: {job_id}'.format(region=region, job_id=dxjob.get_id()),
          file=sys.stderr)
    return dxjob


def _wait_for_completion(jobs):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for j in jobs:
            try:
                j.wait_on_done()
            except DXJobFailureError:
                print("job {} failed".format(j.get_id()))
    finally:
        noise.kill()
    print("done")


def _clone_asset(record, folder, regions, project_dict):
    """
    This function will attempt to clone the given record into all of the given regions.
    It will return a dictionary with the regions as keys and the record-ids of the
    corresponding asset as the values.  If an asset is not able to be created in a given
    region, the value will be set to None.
    """
    # Get the asset record
    fid = record.get_details()['archiveFileId']['$dnanexus_link']
    curr_region = dxpy.describe(record.project)['region']

    # Only run once per region
    regions = set(regions) - set([curr_region])
    if len(regions) == 0:
        # there is nothing to do
        return

    app_supported_regions = set(COPY_FILE_APP.describe()['regionalOptions'].keys())
    if len(regions - app_supported_regions) > 0:
        print('Currently no support for the following region(s): [{regions}]'
              .format(regions=', '.join(regions - app_supported_regions)),
              file=sys.stderr)
        sys.exit(1)

    # Get information about the asset
    asset_properties = record.get_properties()
    asset_properties['cloned_from'] = record.get_id()
    asset_file_name = dxpy.describe(fid)['name']
    url = dxpy.DXFile(fid).get_download_url(preauthenticated=True,
                                            project=dxpy.DXFile.NO_PROJECT_HINT,
                                            duration=URL_DURATION)[0]

    # setup target folders
    region2projid = {}
    for region in regions:
        dest_proj = util.get_project(project_dict[region])
        dest_proj.new_folder(folder, parents=True)
        region2projid[region] = dest_proj.get_id()
    print(region2projid)

    # Fire off a clone process for each region
    jobs = []
    for region in regions:
        dest_proj_id = region2projid[region]
        results = list(dxpy.find_data_objects(classname = "file",
                                              visibility = "hidden",
                                              name = asset_file_name,
                                              project = dest_proj_id,
                                              folder = folder))
        file_ids = [p["id"] for p in results]
        nfiles = len(file_ids)
        if nfiles == 1:
            continue
        if nfiles > 1:
            print("cleanup in {}, found {} files instead of 0/1".format(dest_proj_id, nfiles))
            dxpy.DXProject(dest_proj_id).remove_objects(file_ids)
        dxjob = _clone_asset_into_region(region,
                                         dest_proj_id,
                                         asset_file_name,
                                         folder,
                                         url)
        jobs.append(dxjob)

    # Wait for the cloning to complete
    _wait_for_completion(jobs)

    # make records for each file
    for region in regions:
        dest_proj_id = region2projid[region]
        results = list(dxpy.find_data_objects(classname = "file",
                                              visibility = "hidden",
                                              name = asset_file_name,
                                              project = dest_proj_id,
                                              folder = folder))
        file_ids = [p["id"] for p in results]
        if len(file_ids) == 0:
            raise RuntimeError("Found no files {}:{}/{}".format(dest_proj_id, folder, asset_file_name))
        if len(file_ids) > 1:
            raise RuntimeError("Found {} files {}:{}/{}, instead of just one"
                               .format(len(dxfiles), dest_proj_id, folder, asset_file_name))
        dest_asset = dxpy.new_dxrecord(name=record.name,
                                       types=['AssetBundle'],
                                       details={'archiveFileId': dxpy.dxlink(file_ids[0])},
                                       properties=record.get_properties(),
                                       project=dest_proj_id,
                                       folder=folder,
                                       close=True)


def main():
    argparser = argparse.ArgumentParser(description="Build a dxWDL release")
    argparser.add_argument("--force",
                           help="Build even if there is an existing version",
                           action='store_true',
                           default=False)
    argparser.add_argument("--multi-region",
                           help="Copy to all supported regions",
                           action='store_true',
                           default=False)
    args = argparser.parse_args()

    # build multi-region jar for releases, or
    # if explicitly specified
    multi_region = args.multi_region

    # Choose which dictionary to use
    if multi_region:
        project_dict = RELEASE_DICT
    else:
        project_dict = TEST_DICT

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

    # Build the asset, and the compiler jar file.
    path_dict = dict(map(lambda kv: (kv[0], kv[1] + ":" + folder),
                         project_dict.items()))
    home_ad = util.build(project, folder, version_id, top_dir, path_dict)

    if multi_region:
        home_rec = dxpy.DXRecord(home_ad.asset_id)
        all_regions = project_dict.keys()

        # Leave only regions where the asset is missing
        target_regions = []
        for dest_region in all_regions:
            dest_proj = util.get_project(project_dict[dest_region])
            dest_asset = util.find_asset(dest_proj, folder)
            if dest_asset == None:
                target_regions.append(dest_region)

        _clone_asset(home_rec, folder, target_regions, project_dict)

if __name__ == '__main__':
    main()
