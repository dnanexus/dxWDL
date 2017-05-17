#!/usr/bin/env python
from __future__ import print_function

import argparse
from collections import namedtuple
import dxpy
import fnmatch
import json
import pprint
import os
import sys
import subprocess
import time
from dxpy.exceptions import DXJobFailureError

top_dir = os.path.dirname(sys.argv[0])
test_dir = os.path.join(top_dir, "tests")
git_revision = subprocess.check_output(["git", "describe", "--always", "--dirty", "--tags"]).strip()
git_revision_in_jar= subprocess.check_output(["git", "describe", "--always", "--tags"]).strip()
test_files={}
test_failing=set([])
reserved_test_names=['S', 'M', 'All', 'list']
small_test_list = [
    "system_calls", "var_types", "math_expr",
    "call_expressions2", "string_array", "sg_sum3", "sg_files",
    "ragged_array2", "advanced",
    "file_disambiguation", "file_array",

    # optional arguments
    "optionals",

    # docker
    "bwa_version",

    # lifting declarations
    "decl_mid_wf",

    # Error codes
    "bad_status", "bad_status2"
]

medium_test_list = [
    # Casting
    "cast",

    # Variable instance types
    "instance_types"
] + small_test_list

TestDesc = namedtuple('TestDesc', 'wdl_source wdl_input dx_input results')

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

# Register a test name, find its inputs and expected results files.
def register_test(wf_name):
    if wf_name in reserved_test_names:
        raise Exception("Test name {} is reserved".format(wf_name))
    desc = TestDesc(wdl_source= os.path.join(test_dir, wf_name + ".wdl"),
                    wdl_input= os.path.join(test_dir, wf_name + "_input.json"),
                    dx_input= os.path.join(test_dir, wf_name + "_input.dx.json"),
                    results= os.path.join(test_dir, wf_name + "_results.json"))
    for path in [desc.wdl_source, desc.wdl_input]:
        if not os.path.exists(path):
            raise Exception("Test file {} does not exist".format(path))

    # Verify the validity of the JSON files
    for path in [desc.wdl_input, desc.dx_input, desc.results]:
        if os.path.exists(path):
            verify_json_file(path)
    test_files[wf_name] = desc
    desc

def register_test_fail(wf_name):
    register_test(wf_name)
    test_failing.add(wf_name)

######################################################################

# Same as above, however, if a file is empty, return an empty dictionary
def read_json_file_maybe_empty(path):
    if not os.path.exists(path):
        return {}
    else:
        return read_json_file(path)

def find_stage_outputs_by_name(wf_name, desc, stage_name):
    stages = desc['stages']
    for snum in range(len(stages)):
        crnt = stages[snum]['execution']['name']
        if crnt == stage_name:
            return stages[snum]['execution']['output']
    raise Exception("Analysis {} does not have stage {}".format(wf_name, stage_name))

# Check that a workflow returned the expected result for
# a [key]
def validate_result(wf_name, desc, key, expected_val):
    # Split key into stage-number and name. For example:
    #  '0.count' -> 0, count
    stage_name = key.split('.')[0]
    field_name = key.split('.')[1]
    try:
        # get the actual results
        stage_results = find_stage_outputs_by_name(wf_name, desc, stage_name)
        if field_name not in stage_results:
            print("field {} missing from stage results {}".format(field_name, stage_results))
            return False
        result = stage_results[field_name]
        if type(result) is list:
            result.sort()
            expected_val.sort()
        if result != expected_val:
            print("Analysis {} gave unexpected results".format(wf_name))
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


# Build a workflow.
#
# wf             workflow name
# classpath      java classpath needed for running compilation
# folder         destination folder on the platform
def build_workflow(wf_name, project, folder, asset, compiler_flags):
    print("build workflow {}".format(wf_name))
    test_desc = test_files[wf_name]
    print("Compiling {} to a workflow".format(test_desc.wdl_source))
    cmdline = [ (top_dir + "/dxWDL"),
                "compile",
                test_desc.wdl_source,
                "--wdl_input_file", test_desc.wdl_input,
                "--destination", (project.get_id() + ":" + folder),
                "--asset", asset.get_id() ]
    cmdline += compiler_flags
    subprocess.check_output(cmdline)
    return lookup_workflow(wf_name, project, folder)

def lookup_workflow(wf_name, project, folder):
    wfgen = dxpy.bindings.search.find_data_objects(classname="workflow",
                                                   name=wf_name,
                                                   folder=folder,
                                                   project=project.get_id(),
                                                   limit=1)
    wf = [item for item in wfgen]
    if len(wf) > 0:
        return wf[0]['id']
    return None

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
                desc = anls.describe()
                wf_name = desc["name"].split(' ')[0]
                if wf_name not in test_failing:
                    raise Exception("Analysis {} failed".format(wf_name))
                else:
                    print("Analysis {} failed as expected".format(wf_name))
    finally:
        noise.kill()
    print("done")



# Run [workflow] on several inputs, return the analysis ID.
def run_workflow(project, test_folder, wf_name, wfId, delay_workspace_destruction):
    def once():
        try:
            test_desc = test_files[wf_name]
            inputs = read_json_file_maybe_empty(test_desc.dx_input)
            #print("inputs={}".format(inputs))
            workflow = dxpy.DXWorkflow(project=project.get_id(), dxid=wfId)
            project.new_folder(test_folder, parents=True)
            analysis = workflow.run(inputs,
                                    project=project.get_id(),
                                    folder=test_folder,
                                    name="{} {}".format(wf_name, git_revision),
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
    for wf_name, wfid in workflows.iteritems():
        test_desc = test_files[wf_name]
        print("Running workflow {}".format(wf_name))
        test_job = run_workflow(project, test_folder, wf_name, wfid, delay_workspace_destruction)
        test_analyses.append(test_job)
    print("test analyses: " + ", ".join([a.get_id() for a in test_analyses]))

    if no_wait:
        return

    # Wait for completion
    wait_for_completion(test_analyses)

    print("Verifying analysis results")
    for analysis in test_analyses:
        desc = analysis.describe()
        wf_name = desc["name"].split(' ')[0]
        test_desc = test_files[wf_name]
        output = desc["output"]
        shouldbe = read_json_file_maybe_empty(test_desc.results)
        correct = True
        print("Checking results for workflow {}".format(wf_name))

        for key, expected_val in shouldbe.iteritems():
            correct = validate_result(wf_name, desc, key, expected_val)
        if correct:
            print("Analysis {} passed".format(wf_name))

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

# Register inputs and outputs for all the tests.
# Return the list of temporary files on the platform
def register_all_tests():
    register_test("math")
    register_test("system_calls")
    register_test("four_step")
    register_test("add3")
    register_test("concat")

    # There is a bug in marshaling floats. Hopefully,
    # it will get fixed in future wdl4s releases.
    register_test("var_types")
    register_test("fs")
    register_test("system_calls2")
    register_test("math_expr")
    register_test("string_expr")
    register_test("call_expressions")
    register_test("call_expressions2")
    register_test("files")
    register_test("string_array")
    register_test("file_array")
    register_test("output_array")
    register_test("file_disambiguation")

    # Scatter/gather
    register_test("sg_sum")
    register_test("sg1")
    register_test("sg_sum2")
    register_test("sg_sum3")
    register_test("sg_files")

    # ragged arrays
    register_test("ragged_array")
    register_test("ragged_array2")

    # optionals
    register_test("optionals")

    # docker
    register_test("bwa_version")
    register_test_fail("bad_status")
    register_test_fail("bad_status2")

    # Output error
    register_test_fail("missing_output")

    # combination of featuers
    register_test("advanced")
    register_test("decl_mid_wf")

    # casting types
    register_test("cast")

    # variable instance types
    register_test("instance_types")

    # Massive tests
    register_test("gatk_170412")

#####################################################################
# Program entry point
def main():
    register_all_tests()

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
                           default="S")
    argparser.add_argument("--test-list", help="Print a list of available tests",
                           action="store_true", default=False)
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
    args = argparser.parse_args()

    if args.test_list:
        print_test_list()
        exit(0)
    test_names = choose_tests(args.test)

    if args.folder is None:
        base_folder = build_prerequisits(project, args)
    else:
        # Use existing prebuilt folder
        base_folder = args.folder
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    project = dxpy.DXProject(args.project)
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
        for wf_name in test_names:
            wfid = None
            if args.lazy:
                wfid = lookup_workflow(wf_name, project, applet_folder)
            if wfid is None:
                wfid = build_workflow(wf_name, project, applet_folder, asset, compiler_flags)
            workflows[wf_name] = wfid
            print("workflow({}) = {}".format(wf_name, wfid))
        if not args.compile_only:
            run_workflow_subset(project, workflows, test_folder, args.delay_workspace_destruction, args.no_wait)
    finally:
        print("Test complete")


if __name__ == '__main__':
    main()
