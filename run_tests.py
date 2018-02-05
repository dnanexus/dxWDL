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
import util
from dxpy.exceptions import DXJobFailureError

top_dir = os.path.dirname(sys.argv[0])
test_dir = os.path.join(top_dir, "test")
git_revision = subprocess.check_output(["git", "describe", "--always", "--dirty", "--tags"]).strip()
test_files={}
test_failing=set(["bad_status", "bad_status2", "missing_output"])
reserved_test_names=['M', 'All', 'list']

medium_test_list = [
    # Basics
    "cast", "math", "strings", "files",

    # various advanced features
    "advanced", "conditionals",

    # optional arguments
    "optionals",

    # lifting declarations
    "decl_mid_wf",

    # Error codes
    "bad_status", "bad_status2",

    # Variable instance types
    "instance_types",

    # Maps and pairs.
    "dict",

    # objects
    "complex",

    # calling native dx applets
    "call_native"
]

# Tests with the reorg flags
test_reorg=["files", "math"]
test_defaults=["files", "math"]

test_locked=["conditionals", "advanced", "bad_status", "bad_status2",
             "instance_types", "dict"]

TestMetaData = namedtuple('TestMetaData', 'name kind')
TestDesc = namedtuple('TestDesc', 'name kind wdl_source wdl_input dx_input results')

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
        raise RuntimeError("Error verifying JSON file {}".format(path))

# Search a WDL file with a python regular expression.
# Note this is not 100% accurate.
#
# Look for all tasks and workflows. If there is exactly
# one workflow, this is a WORKFLOW. If there are no
# workflows and exactly one task, this is an APPLET.
task_pattern_re = re.compile(r"^(task)(\s+)(\w+)(\s+){")
wf_pattern_re = re.compile(r"^(workflow)(\s+)(\w+)(\s+){")
def get_metadata(filename):
    workflows = []
    tasks = []
    with open(filename, 'r') as fd:
        for line in fd:
            m = re.match(wf_pattern_re, line)
            if m is not None:
                workflows.append(m.group(3))
            m = re.match(task_pattern_re, line)
            if m is not None:
                tasks.append(m.group(3))
    if len(workflows) > 1:
        raise RuntimeError("WDL file {} has multiple workflows".format(filename))
    if len(workflows) == 1:
        return TestMetaData(name = workflows[0],
                            kind = "workflow")
    assert(len(workflows) == 0)
    if len(tasks) == 1:
        return TestMetaData(name = tasks[0],
                            kind = "applet")
    raise RuntimeError("{} is not a valid WDL test, #tasks={}".format(filename, len(tasks)))

# Register a test name, find its inputs and expected results files.
def register_test(dir_path, tname):
    global test_files
    if tname in reserved_test_names:
        raise RuntimeError("Test name {} is reserved".format(tname))
    wdl_file = os.path.join(dir_path, tname + ".wdl")
    metadata = get_metadata(wdl_file)
    desc = TestDesc(name = metadata.name,
                    kind = metadata.kind,
                    wdl_source= wdl_file,
                    wdl_input= os.path.join(test_dir, tname + "_input.json"),
                    dx_input= os.path.join(test_dir, tname + "_input.dx.json"),
                    results= os.path.join(test_dir, tname + "_results.json"))
    for path in [desc.wdl_source, desc.wdl_input]:
        if not os.path.exists(path):
            raise RuntimeError("Test file {} does not exist".format(path))

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

def find_test_from_exec(exec_obj):
    dx_desc = exec_obj.describe()
    exec_name = dx_desc["name"].split(' ')[0]
    for tname, desc in test_files.iteritems():
        if desc.name == exec_name:
            return tname
    raise RuntimeError("Test for {} {} not found".format(exec_obj, exec_name))

# Check that a workflow returned the expected result for
# a [key]
def validate_result(tname, exec_outputs, key, expected_val):
    desc = test_files[tname]
    # Extract the key. For example, for workflow "math" returning
    # output "count":
    #    'math.count' -> count
    exec_name = key.split('.')[0]
    field_name = key.split('.')[1]
    if exec_name != tname:
        raise RuntimeError("Key {} is invalid, must start with {} name".format(key, desc.kind))
    try:
        # get the actual results
        if field_name not in exec_outputs:
            print("field {} missing from executable results {}".format(field_name, exec_outputs))
            return False
        result = exec_outputs[field_name]
        if ((type(result) is list) and
            (type(expected_val) is list)):
            result.sort()
            expected_val.sort()
        if result != expected_val:
            print("Analysis {} gave unexpected results".format(tname))
            print("Field {} should be {}, actual = {}".format(field_name, expected_val, result))
            return False
        return True
    except Exception, e:
        print("exception message={}".format(e))
        return False


def lookup_dataobj(tname, project, folder):
    desc = test_files[tname]
    wfgen = dxpy.bindings.search.find_data_objects(classname= desc.kind,
                                                   name= desc.name,
                                                   folder= folder,
                                                   project= project.get_id(),
                                                   limit= 1)
    objs = [item for item in wfgen]
    if len(objs) > 0:
        return objs[0]['id']
    return None

# Build a workflow.
#
# wf             workflow name
# classpath      java classpath needed for running compilation
# folder         destination folder on the platform
def build_test(tname, project, folder, version_id, compiler_flags):
    desc = test_files[tname]
    print("build {} {}".format(desc.kind, desc.name))
    print("Compiling {} to a {}".format(desc.wdl_source, desc.kind))
    cmdline = [ "java", "-jar",
                os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                "compile",
                desc.wdl_source,
                "-folder", folder,
                "-project", project.get_id() ]
    cmdline += compiler_flags
    print(" ".join(cmdline))
    subprocess.check_output(cmdline)
    return lookup_dataobj(tname, project, folder)

def ensure_dir(path):
    print("making sure that {} exists".format(path))
    if not os.path.exists(path):
        os.makedirs(path)

def wait_for_completion(test_exec_objs):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for exec_obj in test_exec_objs:
            try:
                exec_obj.wait_on_done()
            except DXJobFailureError:
                tname = find_test_from_exec(exec_obj)
                desc = test_files[tname]
                if tname not in test_failing:
                    raise RuntimeError("Executable {} failed".format(desc.name))
                else:
                    print("Executable {} failed as expected".format(desc.name))
    finally:
        noise.kill()
    print("done")


# Run [workflow] on several inputs, return the analysis ID.
def run_executable(project, test_folder, tname, oid, delay_workspace_destruction):
    def once():
        try:
            desc = test_files[tname]
            if tname in test_defaults:
                inputs = {}
            else:
                inputs = read_json_file_maybe_empty(desc.dx_input)
            project.new_folder(test_folder, parents=True)
            if desc.kind == "workflow":
                exec_obj = dxpy.DXWorkflow(project=project.get_id(), dxid=oid)
            elif desc.kind == "applet":
                exec_obj = dxpy.DXApplet(project=project.get_id(), dxid=oid)
            else:
                raise RuntimeError("Unknown kind {}".format(desc.kind))
            return exec_obj.run(inputs,
                                project=project.get_id(),
                                folder=test_folder,
                                name="{} {}".format(desc.name, git_revision),
                                delay_workspace_destruction=delay_workspace_destruction,
                                instance_type="mem1_ssd1_x4")
        except Exception, e:
            print("exception message={}".format(e))
            return None

    for i in range(1,5):
        retval = once()
        if retval is not None:
            return retval
        print("Sleeping for 5 seconds before trying again")
        time.sleep(5)
    raise RuntimeError("running workflow")

def extract_outputs(tname, exec_obj):
    desc = test_files[tname]
    if desc.kind == "workflow":
        locked = tname in test_locked
        if locked:
            return exec_obj['output']
        else:
            stages = exec_obj['stages']
            for snum in range(len(stages)):
                crnt = stages[snum]
                if crnt['id'] == 'stage-last':
                    return stages[snum]['execution']['output']
            raise RuntimeError("Analysis for test {} does not have stage 'last'".format(tname))
    elif desc.kind == "applet":
        return exec_obj['output']
    else:
        raise RuntimeError("Unknown kind {}".format(desc.kind))

def run_test_subset(project, runnable, test_folder, delay_workspace_destruction, no_wait):
    # Run the workflows
    test_exec_objs=[]
    for tname, oid in runnable.iteritems():
        desc = test_files[tname]
        print("Running {} {} {}".format(desc.kind, desc.name, oid))
        anl = run_executable(project, test_folder, tname, oid, delay_workspace_destruction)
        test_exec_objs.append(anl)
    print("executables: " + ", ".join([a.get_id() for a in test_exec_objs]))

    if no_wait:
        return

    # Wait for completion
    wait_for_completion(test_exec_objs)

    print("Verifying results")
    for exec_obj in test_exec_objs:
        exec_desc = exec_obj.describe()
        tname = find_test_from_exec(exec_obj)
        if tname in test_failing:
            continue
        test_desc = test_files[tname]
        exec_outputs = extract_outputs(tname, exec_desc)
        shouldbe = read_json_file_maybe_empty(test_desc.results)
        correct = True
        print("Checking results for workflow {}".format(test_desc.name))

        for key, expected_val in shouldbe.iteritems():
            correct = validate_result(tname, exec_outputs, key, expected_val)
        if correct:
            print("Analysis {} passed".format(tname))

def print_test_list():
    l = [key for key in test_files.keys()]
    l.sort()
    ls = "\n  ".join(l)
    print("List of tests:\n  {}".format(ls))

# Choose set set of tests to run
def choose_tests(test_name):
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
        raise RuntimeError("Too many matches for test prefix {} -> {}".format(test_name, matches))
    if len(matches) == 0:
        raise RuntimeError("Test prefix {} is unknown".format(test_name))
    return matches

# Find all the WDL test files, these are located in the 'test'
# directory. A test file must have some support files.
def register_all_tests():
    for root, dirs, files in os.walk(test_dir):
        for t_file in files:
            if t_file.endswith(".wdl"):
                base = os.path.basename(t_file)
                fname = os.path.splitext(base)[0]
                try:
                    register_test(root, fname)
                except Exception, e:
                    print("Skipping WDL file {} error={}".format(fname, e))


def build_dirs(project):
    base_folder = time.strftime("/builds/%Y-%m-%d/%H%M%S-") + git_revision
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    project.new_folder(test_folder, parents=True)
    project.new_folder(applet_folder, parents=True)
    return base_folder

# Some compiler flags are test specific
def compiler_per_test_flags(tname):
    flags = []
    desc = test_files[tname]
    if tname in test_locked:
        flags.append("-locked")
    if tname in test_reorg:
        flags.append("-reorg")
    if tname in test_defaults:
        flags.append("-defaults")
        flags.append(desc.wdl_input)
    else:
        flags.append("-inputs")
        flags.append(desc.wdl_input)
    return flags

######################################################################
# Set up the native calling tests
def native_call_setup(project, applet_folder, version_id):
    # build the native applets
    native_applets = ["native_concat",
                      "native_diff",
                      "native_mk_list",
                      "native_sum",
                      "native_sum_012"]
    for napl in native_applets:
        try:
            cmdline = [ "dx", "build",
                        os.path.join(top_dir, "test/applets/{}".format(napl)),
                        "--destination", (project.get_id() + ":" + applet_folder + "/") ]
            print(" ".join(cmdline))
            subprocess.check_output(cmdline)
        except Exception, e:
            print("Applet {} already exists".format(napl))

    # build WDL wrapper tasks in test/dx_extern.wdl
    cmdline = [ "java", "-jar",
                os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                "dxni",
                "--force",
                "--verbose",
                "--folder", applet_folder,
                "--project", project.get_id(),
                "--output", os.path.join(top_dir, "test/dx_extern.wdl")]
    print(" ".join(cmdline))
    subprocess.check_output(cmdline)

######################################################################
## Program entry point
def main():
    global test_locked
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    argparser.add_argument("--archive", help="Archive old applets",
                           action="store_true", default=False)
    argparser.add_argument("--compile-only", help="Only compile the workflows, don't run them",
                           action="store_true", default=False)
    argparser.add_argument("--compile-mode", help="Compilation mode")
    argparser.add_argument("--delay-workspace-destruction", help="Flag passed to workflow run",
                           action="store_true", default=False)
    argparser.add_argument("--force", help="Remove old versions of applets and workflows",
                           action="store_true", default=False)
    argparser.add_argument("--folder", help="Use an existing folder, instead of building dxWDL")
    argparser.add_argument("--lazy", help="Only compile workflows that are unbuilt",
                           action="store_true", default=False)
    argparser.add_argument("--locked", help="Generate locked-down workflows",
                           action="store_true", default=False)
    argparser.add_argument("--regular", help="Generate only regular workflows",
                           action="store_true", default=False)
    argparser.add_argument("--no-wait", help="Exit immediately after launching tests",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAnexus project ID",
                           default="project-F07pBj80ZvgfzQK28j35Gj54")
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
        base_folder = build_dirs(project)
    else:
        # Use existing prebuilt folder
        base_folder = args.folder
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    print("project: {} ({})".format(project.name, args.project))
    print("folder: {}".format(base_folder))

    # build the dxWDL jar file, only on us-east-1
    version_id = util.get_version_id(top_dir)
    if args.folder is None:
        home_ad = util.build(project, applet_folder, version_id, top_dir)
        jar_path = util.build_final_jar(version_id, top_dir, [home_ad])

    if args.regular:
        # Disable all locked workflows
        args.locked = False
        test_locked = []

    compiler_flags = []
    if args.locked:
        compiler_flags.append("-locked")
        test_locked += test_names
    if args.archive:
        compiler_flags.append("-archive")
    if args.compile_mode:
        compiler_flags += ["-compileMode", args.compile_mode]
    if args.force:
        compiler_flags.append("-force")
    if args.verbose:
        compiler_flags.append("-verbose")

    if "call_native" in test_names:
        native_call_setup(project, applet_folder, version_id)

    try:
        # Compile the WDL files to dx:workflows and dx:applets
        runnable = {}
        for tname in test_names:
            oid = None
            if args.lazy:
                oid = lookup_dataobj(tname, project, applet_folder)
            if oid is None:
                c_flags = compiler_flags[:] + compiler_per_test_flags(tname)
                oid = build_test(tname, project, applet_folder, version_id, c_flags)
            runnable[tname] = oid
            print("runnable({}) = {}".format(tname, oid))
        if not args.compile_only:
            run_test_subset(project, runnable, test_folder,
                            args.delay_workspace_destruction,
                            args.no_wait)
    finally:
        print("Test complete")


if __name__ == '__main__':
    main()
