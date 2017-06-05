#!/usr/bin/env python
from __future__ import print_function

import argparse
from collections import namedtuple
import dxpy
import fnmatch
import json
import pprint
import os
import re
import sys
import subprocess
import time
from dxpy.exceptions import DXJobFailureError

top_dir = os.path.dirname(sys.argv[0])
test_dir = os.path.join(top_dir, "test")
git_revision = subprocess.check_output(["git", "describe", "--always", "--dirty", "--tags"]).strip()
git_revision_in_jar= subprocess.check_output(["git", "describe", "--always", "--tags"]).strip()
test_files={}
test_failing=set([])
reserved_test_names=['S', 'M', 'All', 'list']
small_test_list = [
    "system_calls",
    "sg_sum3", "sg_files",
    "ragged_array2",
    "file_array"
]

medium_test_list = [
    # basics
    "var_types", "math_expr", "call_expressions2",

    # File path handling, and files with the same name
    "file_disambiguation", "files",

    # various advanced features
    "advanced",

    # optional arguments
    "optionals",

    # docker
    "bwa_version",

    # lifting declarations
    "decl_mid_wf",

    # Error codes
    "bad_status", "bad_status2",

    # Casting
    "cast",

    # Variable instance types
    "instance_types",

    # Complex data types
    "file_ragged_array"
] + small_test_list

TestDesc = namedtuple('TestDesc', 'wf_name wdl_source wdl_input dx_input results')

######################################################################
# Read a JSON file
def read_json_file(path):
    with open(path, 'r') as fd:
        data = fd.read()
        d = json.loads(data)
        return d

def verify_json_file(path):
    try:
        read_json_file(path)
    except:
        raise Exception("Error verifying JSON file {}".format(path))

# Extract the workflow name from a WDL source file.
# Shell out to scala.
def get_workflow_name_slow(wdl_file):
    print("Getting workflow name from {}".format(wdl_file))
    cmdline = [ (top_dir + "/dxWDL"), "workflowName", wdl_file ]
    outstr = subprocess.check_output(cmdline)
    return outstr.rstrip().split('\n')[-1].strip()

# Search a WDL file with a python regular expression.
# This is much faster, but less accurate
wf_pattern_re = re.compile(r"^(workflow)(\s+)(\w+)(\s+){")
def get_workflow_name_fast(filename):
    with open(filename, 'r') as fd:
        for line in fd:
            m = re.match(wf_pattern_re, line)
            if m is not None:
                return m.group(3)
    raise Exception("Workflow name not found")

# Register a test name, find its inputs and expected results files.
def register_test(tname):
    if tname in reserved_test_names:
        raise Exception("Test name {} is reserved".format(tname))
    wdl_file = os.path.join(test_dir, tname + ".wdl")
    wf_name = get_workflow_name_fast(wdl_file)
    desc = TestDesc(wf_name = wf_name,
                    wdl_source= wdl_file,
                    wdl_input= os.path.join(test_dir, tname + "_input.json"),
                    dx_input= os.path.join(test_dir, tname + "_input.dx.json"),
                    results= os.path.join(test_dir, tname + "_results.json"))
    for path in [desc.wdl_source, desc.wdl_input]:
        if not os.path.exists(path):
            raise Exception("Test file {} does not exist".format(path))

    # Verify the validity of the JSON files
    for path in [desc.wdl_input, desc.dx_input, desc.results]:
        if os.path.exists(path):
            verify_json_file(path)
    test_files[tname] = desc
    desc

######################################################################

# Same as above, however, if a file is empty, return an empty dictionary
def read_json_file_maybe_empty(path):
    if not os.path.exists(path):
        return {}
    else:
        return read_json_file(path)

def find_stage_outputs_by_name(tname, desc, stage_name):
    stages = desc['stages']
    for snum in range(len(stages)):
        crnt = stages[snum]['execution']['name']
        if crnt == stage_name:
            return stages[snum]['execution']['output']
    raise Exception("Analysis for test {} does not have stage {}".format(tname, stage_name))

def find_test_from_analysis(analysis):
    anl_desc = analysis.describe()
    wf_name = anl_desc["name"].split(' ')[0]
    for tname, desc in test_files.iteritems():
        if desc.wf_name == wf_name:
            return tname
    raise Exception("Test for workflow {} not found".format(wf_name))

# Check that a workflow returned the expected result for
# a [key]
def validate_result(tname, analysis_desc, key, expected_val):
    # Split key into stage-number and name. For example:
    #  '0.count' -> 0, count
    stage_name = key.split('.')[0]
    field_name = key.split('.')[1]
    try:
        # get the actual results
        stage_results = find_stage_outputs_by_name(tname, analysis_desc, stage_name)
        if field_name not in stage_results:
            print("field {} missing from stage results {}".format(field_name, stage_results))
            return False
        result = stage_results[field_name]
        if type(result) is list:
            result.sort()
            expected_val.sort()
        if result != expected_val:
            print("Analysis {} gave unexpected results".format(tname))
            print("stage={}".format(stage_name))
            print("stage_results={}".format(stage_results))
            print("Should be stage[{}].{} = {} , actual = {}".format(stage_name, field_name, expected_val, result))
            return False
        return True
    except Exception, e:
        #print("no stage {} in results {}".format(stage_name, desc['stages']))
        print("exception message={}".format(e))
        return False

def build_prerequisits(project, args):
    base_folder = time.strftime("/builds/%Y-%m-%d/%H%M%S-") + git_revision
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    project.new_folder(test_folder, parents=True)
    project.new_folder(applet_folder, parents=True)

    # Run make, to ensure that we have an up-to-date jar file
    #
    # Be careful, so that the make invocation will work even if called from a different
    # directory.
    print("Calling make")
    subprocess.check_call(["make", "-C", top_dir, "all"])
    print("")

    # Create an asset from the dxWDL jar file and its dependencies, this speeds up applet creation.
    print("Creating an asset from dxWDL")
    subprocess.check_call(["dx", "build_asset", "applet_resources",
                           "--destination",
                           project.get_id() + ":" + applet_folder + "/dxWDLrt"])
    print("")
    return base_folder

def lookup_workflow(tname, project, folder):
    desc = test_files[tname]
    wfgen = dxpy.bindings.search.find_data_objects(classname="workflow",
                                                   name=desc.wf_name,
                                                   folder=folder,
                                                   project=project.get_id(),
                                                   limit=1)
    wf = [item for item in wfgen]
    if len(wf) > 0:
        return wf[0]['id']
    return None

# Build a workflow.
#
# wf             workflow name
# classpath      java classpath needed for running compilation
# folder         destination folder on the platform
def build_workflow(tname, project, folder, asset, compiler_flags):
    desc = test_files[tname]
    print("build workflow {}".format(desc.wf_name))
    print("Compiling {} to a workflow".format(desc.wdl_source))
    cmdline = [ (top_dir + "/dxWDL"),
                "compile",
                desc.wdl_source,
                "--wdl_input_file", desc.wdl_input,
                "--destination", (project.get_id() + ":" + folder),
                "--asset", asset.get_id() ]
    cmdline += compiler_flags
    subprocess.check_output(cmdline)
    return lookup_workflow(tname, project, folder)

def ensure_dir(path):
    print("making sure that {} exists".format(path))
    if not os.path.exists(path):
        os.makedirs(path)

def wait_for_completion(test_analyses):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for anls in test_analyses:
            try:
                anls.wait_on_done()
            except DXJobFailureError:
                tname = find_test_from_analysis(anls)
                desc = test_files[tname]
                if tname not in test_failing:
                    raise Exception("Analysis {} failed".format(desc.wf_name))
                else:
                    print("Analysis {} failed as expected".format(desc.wf_name))
    finally:
        noise.kill()
    print("done")



# Run [workflow] on several inputs, return the analysis ID.
def run_workflow(project, test_folder, tname, wfId, delay_workspace_destruction):
    def once():
        try:
            desc = test_files[tname]
            inputs = read_json_file_maybe_empty(desc.dx_input)
            #print("inputs={}".format(inputs))
            workflow = dxpy.DXWorkflow(project=project.get_id(), dxid=wfId)
            project.new_folder(test_folder, parents=True)
            analysis = workflow.run(inputs,
                                    project=project.get_id(),
                                    folder=test_folder,
                                    name="{} {}".format(desc.wf_name, git_revision),
                                    delay_workspace_destruction=delay_workspace_destruction)
            return analysis
        except Exception, e:
            print("exception message={}".format(e))
            return None

    for i in range(1,5):
        retval = once()
        if retval is not None:
            return retval
        print("Sleeping for 5 seconds before trying again")
        time.sleep(5)
    raise ("Error running workflow")

def run_workflow_subset(project, workflows, test_folder, delay_workspace_destruction, no_wait):
    # Run the workflows
    test_analyses=[]
    for tname, wfid in workflows.iteritems():
        desc = test_files[tname]
        print("Running workflow {}".format(desc.wf_name))
        test_job = run_workflow(project, test_folder, tname, wfid, delay_workspace_destruction)
        test_analyses.append(test_job)
    print("test analyses: " + ", ".join([a.get_id() for a in test_analyses]))

    if no_wait:
        return

    # Wait for completion
    wait_for_completion(test_analyses)

    print("Verifying analysis results")
    for analysis in test_analyses:
        analysis_desc = analysis.describe()
        tname = find_test_from_analysis(analysis)
        test_desc = test_files[tname]
        output = analysis_desc["output"]
        shouldbe = read_json_file_maybe_empty(test_desc.results)
        correct = True
        print("Checking results for workflow {}".format(test_desc.wf_name))

        for key, expected_val in shouldbe.iteritems():
            correct = validate_result(tname, analysis_desc, key, expected_val)
        if correct:
            print("Analysis {} passed".format(tname))

def print_test_list():
    l = [key for key in test_files.keys()]
    l.sort()
    ls = "\n  ".join(l)
    print("List of tests:\n  {}".format(ls))

# Choose set set of tests to run
def choose_tests(test_name):
    if test_name == 'S':
        return small_test_list
    if test_name == 'M':
        return medium_test_list
    if test_name == 'All':
        return test_files.keys()
    if test_name in test_files.keys():
        return [test_name]
    # Last chance: check if the name is a prefix.
    # Accept it if there is exactly a single match.
    matches = [key for key in test_files.keys() if key.startswith(test_name)]
    if len(matches) > 1:
        raise Exception("Too many matches for test prefix {} -> {}".format(test_name, matches))
    if len(matches) == 0:
        raise Exception("Test prefix {} is unknown".format(test_name))
    return matches

# Find all the WDL test files, these are located in the 'test'
# directory. A test file must have some support files.
def register_all_tests():
    for t_file in os.listdir(test_dir):
        if t_file.endswith(".wdl"):
            base = os.path.basename(t_file)
            fname = os.path.splitext(base)[0]
            try:
                register_test(fname)
            except Exception, e:
                print("Skipping WDL file {}".format(fname))

    # failing tests
    test_failing.add("bad_status")
    test_failing.add("bad_status2")
    test_failing.add("missing_output")

######################################################################
## Program entry point
def main():
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    argparser.add_argument("--compile-only", help="Only compile the workflows, don't run them",
                           action="store_true", default=False)
    argparser.add_argument("--compile-mode", help="Compilation mode")
    argparser.add_argument("--delay-workspace-destruction", help="Flag passed to workflow run",
                           action="store_true", default=False)
    argparser.add_argument("--force", help="Rebuild all the applets and workflows",
                           action="store_true", default=False)
    argparser.add_argument("--folder", help="Use an existing folder, instead of building dxWDL")
    argparser.add_argument("--lazy", help="Only compile workflows that are unbuilt",
                           action="store_true", default=False)
    argparser.add_argument("--no-wait", help="Exit immediately after launching tests",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAnexus project ID", default="project-F07pBj80ZvgfzQK28j35Gj54")
    argparser.add_argument("--test", help="Run a test, or a subgroup of tests",
                           action="append", default=[])
    argparser.add_argument("--test-list", help="Print a list of available tests",
                           action="store_true", default=False)
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
    args = argparser.parse_args()

    register_all_tests()
    if args.test_list:
        print_test_list()
        exit(0)
    test_names = []
    if len(args.test) == 0:
        args.test = 'M'
    for t in args.test:
        test_names += choose_tests(t)
    print("Running tests {}".format(test_names))

    project = dxpy.DXProject(args.project)
    if args.folder is None:
        base_folder = build_prerequisits(project, args)
    else:
        # Use existing prebuilt folder
        base_folder = args.folder
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    print("project: {} ({})".format(project.name, args.project))
    print("folder: {}".format(base_folder))

    compiler_flags=[]
    if args.verbose:
        compiler_flags.append("--verbose")
    if args.compile_mode:
        compiler_flags += ["--mode", args.compile_mode]
    if args.force:
        compiler_flags.append("--force")

    # Move the record to the applet_folder, so the compilation process will find it
    # Output: asset_bundle = record-F13V3BQ05gjppZPy1QyKxXzq
    # Find the asset
    asset = dxpy.search.find_one_data_object(classname="record",
                                             project=project.get_id(),
                                             name="dxWDLrt",
                                             folder=base_folder,
                                             return_handler=True,
                                             more_ok=False)
    print("asset_id={}".format(asset.get_id()))

    try:
        # Compile the WDL workflows
        workflows = {}
        for tname in test_names:
            wfid = None
            if args.lazy:
                wfid = lookup_workflow(tname, project, applet_folder)
            if wfid is None:
                wfid = build_workflow(tname, project, applet_folder, asset, compiler_flags)
            workflows[tname] = wfid
            print("workflow({}) = {}".format(tname, wfid))
        if not args.compile_only:
            run_workflow_subset(project, workflows, test_folder, args.delay_workspace_destruction, args.no_wait)
    finally:
        print("Test complete")


if __name__ == '__main__':
    main()
