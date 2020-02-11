#!/usr/bin/env python
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
from typing import Callable, Iterator, Union, Optional, List
from termcolor import colored, cprint
import time
from dxpy.exceptions import DXJobFailureError

import util

here = os.path.dirname(sys.argv[0])
top_dir = os.path.dirname(os.path.abspath(here))
test_dir = os.path.join(os.path.abspath(top_dir), "test")

git_revision = subprocess.check_output(["git", "describe", "--always", "--dirty", "--tags"]).strip()
test_files={}
test_failing=set(["bad_status",
                  "bad_status2",
                  "just_fail_wf",
                  "missing_output",
                  "docker_retry"
                  ])

wdl_v1_list = [
     # calling native dx applets/apps
    "call_native_v1",
    "call_native_app",

    "cast",
    "dict",
    "instance_types",
    "linear_no_expressions",
    "linear",
    "optionals",

    "spaces_in_file_paths",
    "strings",

    # workflows with nested blocks
    "two_levels",
    "param_passing",
    "nested_scatter",

    # Map with a File key
    "map_file_key",

    # defaults and parameter passing
    "top",
    "subworkflow_with_default",

    # can we download from a container?
    "download_from_container",

    # input file with pairs
    "echo_pairs",
    "array_structs",

    # Missing optional output files, returned as none, instead
    # of an error
    "missing_optional_output_file",

    # calling with an optional argument not specified
    "scatter_subworkflow_with_optional",

    # streaming
    "streaming_inputs"
]

# docker image tests
docker_test_list = [
    "broad_genomics",
    "biocontainers",
    "private_registry",
    "native_docker_file_image",
    "native_docker_file_image_gzip",
    "samtools_count",
    "hostname_is_jobid",
]

# wdl draft-2
draft2_test_list = [
    "advanced",
    "bad_status",
    "bad_status2",
    "just_fail_wf",
    "call_with_defaults1",
    "call_with_defaults2",
    "conditionals_base",
    "files",
    "files_with_the_same_name",
    "hello",
    "shapes",

    # multiple library imports in one WDL workflow
    "multiple_imports",

    # subworkflows
    "conditionals2",
    "modulo",
    "movies",
    "subblocks2",
    "subblocks",
    "var_type_change",

     # calling native dx applets/apps
    "call_native"
]

single_tasks_list = [
    "add3",
    "diff2files",
    "empty_stdout",
    "sort_file"
]

# Tests run in continuous integration. We remove the native app test,
# because we don't want to give permissions for creating platform apps.
ci_test_list = [
    "advanced",
    "call_native",
    "call_with_defaults1",
    "trains",
    "files"
]

special_flags_list = [
    "add2",      # test the ignoreReuse flag
    "add_many"   # tests the delayWorkflowDestruction flag
]

medium_test_list= wdl_v1_list + docker_test_list + special_flags_list
large_test_list= medium_test_list + draft2_test_list + single_tasks_list

test_suites = {
    'CI': ci_test_list,
    'M': medium_test_list,
    'L': large_test_list,
    'tasks' : single_tasks_list,
    'draft2': draft2_test_list,
    'docker': docker_test_list,
    'native': ["call_native", "call_native_v1"]
}

# Tests with the reorg flags
test_reorg=["dict", "strings"]
test_defaults=[]
test_unlocked=["array_structs",
               "cast",
               "call_with_defaults1",
               "files",
               "hello",
               "path_not_taken",
               "optionals",
               "shapes"]

test_import_dirs=["A"]
TestMetaData = namedtuple('TestMetaData', ['name','kind'])
TestDesc = namedtuple('TestDesc',
                      ['name', 'kind', 'wdl_source', 'wdl_input', 'dx_input', 'results', 'extras'])

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
    if (os.path.basename(filename).startswith("library") or
        os.path.basename(filename).endswith("_extern")):
        return
    raise RuntimeError("{} is not a valid WDL test, #tasks={}".format(filename, len(tasks)))

# Register a test name, find its inputs and expected results files.
def register_test(dir_path, tname):
    global test_files
    if tname in test_suites.keys():
        raise RuntimeError("Test name {} is already used by a test-suite, it is reserved".format(tname))
    wdl_file = os.path.join(dir_path, tname + ".wdl")
    if not os.path.exists(wdl_file):
        raise RuntimeError("Test file {} does not exist".format(path))
    metadata = get_metadata(wdl_file)
    desc = TestDesc(name = metadata.name,
                    kind = metadata.kind,
                    wdl_source= wdl_file,
                    wdl_input= None,
                    dx_input= None,
                    results= os.path.join(dir_path, tname + "_results.json"),
                    extras = None)

    # Verify the input file, and add it (if it exists)
    wdl_input= os.path.join(dir_path, tname + "_input.json")
    if os.path.exists(wdl_input):
        verify_json_file(wdl_input)
        desc = desc._replace(wdl_input= wdl_input,
                             dx_input= os.path.join(dir_path, tname + "_input.dx.json"))

    # Add an extras file (if it exists)
    extras = os.path.join(dir_path, tname + "_extras.json")
    if os.path.exists(extras):
        desc = desc._replace(extras = extras)

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
    for tname, desc in test_files.items():
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
    field_name_parts = key.split('.')[1:]

    # convert dots to ___
    field_name = "___".join(field_name_parts)
    if exec_name != tname:
        raise RuntimeError("Key {} is invalid, must start with {} name".format(key, desc.kind))
    try:
        # get the actual results
        if field_name not in exec_outputs:
            cprint("field {} missing from executable results {}".format(field_name, exec_outputs),
                   "red")
            return False
        result = exec_outputs[field_name]
        if ((type(result) is list) and
            (type(expected_val) is list)):
            result.sort()
            expected_val.sort()
        if result != expected_val:
            cprint("Analysis {} gave unexpected results".format(tname),
                   "red")
            cprint("Field {} should be {}, actual = {}".format(field_name, expected_val, result),
                   "red")
            return False
        return True
    except Exception as e:
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
    oid = subprocess.check_output(cmdline).strip()
    return oid.decode("ascii")

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
                if tname in test_failing:
                    print("Executable {} failed as expected".format(desc.name))
                else:
                    raise RuntimeError("Executable {} failed".format(desc.name))
    finally:
        noise.kill()
    print("done")


# Run [workflow] on several inputs, return the analysis ID.
def run_executable(project, test_folder, tname, oid, debug_flag):
    def once():
        try:
            desc = test_files[tname]
            if tname in test_defaults:
                inputs = {}
            elif desc.dx_input is None:
                inputs = {}
            else:
                inputs = read_json_file(desc.dx_input)
            project.new_folder(test_folder, parents=True)
            if desc.kind == "workflow":
                exec_obj = dxpy.DXWorkflow(project=project.get_id(), dxid=oid)
            elif desc.kind == "applet":
                exec_obj = dxpy.DXApplet(project=project.get_id(), dxid=oid)
            else:
                raise RuntimeError("Unknown kind {}".format(desc.kind))

            run_kwargs = {}
            if debug_flag:
                run_kwargs = {
                    "debug": {"debugOn": ['AppError', 'AppInternalError', 'ExecutionError'] },
                    "allow_ssh" : [ "*" ]
                }

            return exec_obj.run(inputs,
                                project=project.get_id(),
                                folder=test_folder,
                                name="{} {}".format(desc.name, git_revision),
                                instance_type="mem1_ssd1_x4",
                                **run_kwargs)
        except Exception as e:
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
        locked = tname not in test_unlocked
        if locked:
            return exec_obj['output']
        else:
            stages = exec_obj['stages']
            for snum in range(len(stages)):
                crnt = stages[snum]
                if crnt['id'] == 'stage-outputs':
                    return stages[snum]['execution']['output']
            raise RuntimeError("Analysis for test {} does not have stage 'outputs'".format(tname))
    elif desc.kind == "applet":
        return exec_obj['output']
    else:
        raise RuntimeError("Unknown kind {}".format(desc.kind))

def run_test_subset(project, runnable, test_folder, debug_flag):
    # Run the workflows
    test_exec_objs=[]
    for tname, oid in runnable.items():
        desc = test_files[tname]
        print("Running {} {} {}".format(desc.kind, desc.name, oid))
        anl = run_executable(project, test_folder, tname, oid, debug_flag)
        test_exec_objs.append(anl)
    print("executables: " + ", ".join([a.get_id() for a in test_exec_objs]))

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

        for key, expected_val in shouldbe.items():
            correct = validate_result(tname, exec_outputs, key, expected_val)
        if correct:
            print("Analysis {} passed".format(tname))

def print_test_list():
    l = [key for key in test_files.keys()]
    l.sort()
    ls = "\n  ".join(l)
    print("List of tests:\n  {}".format(ls))

# Choose set set of tests to run
def choose_tests(name):
    if name in test_suites.keys():
        return test_suites[name]
    if name == 'All':
        return test_files.keys()
    if name in test_files.keys():
        return [name]
    # Last chance: check if the name is a prefix.
    # Accept it if there is exactly a single match.
    matches = [key for key in test_files.keys() if key.startswith(name)]
    if len(matches) > 1:
        raise RuntimeError("Too many matches for test prefix {} -> {}".format(name, matches))
    if len(matches) == 0:
        raise RuntimeError("Test prefix {} is unknown".format(name))
    return matches

# Find all the WDL test files, these are located in the 'test'
# directory. A test file must have some support files.
def register_all_tests(verbose : bool) -> None :
    for root, dirs, files in os.walk(test_dir):
        for t_file in files:
            if t_file.endswith(".wdl"):
                base = os.path.basename(t_file)
                fname = os.path.splitext(base)[0]
                if fname.startswith("library_"):
                    continue
                if fname.endswith("_extern"):
                    continue
                try:
                    register_test(root, fname)
                except Exception as e:
                    if verbose:
                        print("Skipping WDL file {} error={}".format(fname, e))


# Some compiler flags are test specific
def compiler_per_test_flags(tname):
    flags = []
    desc = test_files[tname]
    if tname not in test_unlocked:
        flags.append("-locked")
    if tname in test_reorg:
        flags.append("-reorg")
    if tname in test_defaults:
        flags.append("-defaults")
        flags.append(desc.wdl_input)
    else:
        if desc.wdl_input is not None:
            flags.append("-inputs")
            flags.append(desc.wdl_input)
    if desc.extras is not None:
        flags += ["--extras", os.path.join(top_dir, desc.extras)]
    if tname in test_import_dirs:
        flags += ["--imports", os.path.join(top_dir, "test/imports/lib")]
    return flags

# Which project to use for a test
# def project_for_test(tname):

######################################################################

def native_call_dxni(project, applet_folder, version_id, verbose: bool):
    # build WDL wrapper tasks in test/dx_extern.wdl
    cmdline_common = [ "java", "-jar",
                       os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                       "dxni",
                       "--force",
                       "--folder", applet_folder,
                       "--project", project.get_id()]
    if verbose:
        cmdline_common.append("--verbose")

    cmdline_draft2 = cmdline_common + [ "--language", "wdl_draft2",
                                        "--output", os.path.join(top_dir, "test/draft2/dx_extern.wdl")]
    print(" ".join(cmdline_draft2))
    subprocess.check_output(cmdline_draft2)

    cmdline_v1 = cmdline_common + [ "--language", "wdl_v1.0",
                                    "--output", os.path.join(top_dir, "test/wdl_1_0/dx_extern.wdl")]
    print(" ".join(cmdline_v1))
    subprocess.check_output(cmdline_v1)


def native_one_call_dxni(project, path, version_id, verbose):
    # build WDL wrapper tasks in test/dx_extern.wdl
    cmdline_common = [ "java", "-jar",
                       os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                       "dxni",
                       "--force",
                       "--path", path,
                       "--project", project.get_id()]
    if verbose:
        cmdline_common.append("--verbose")
    cmdline_v1 = cmdline_common + [ "--language", "wdl_v1.0",
                                    "--output", os.path.join(top_dir, "test/wdl_1_0/dx_extern_one.wdl")]
    print(" ".join(cmdline_v1))
    subprocess.check_output(cmdline_v1)

# Set up the native calling tests
def native_call_setup(project, applet_folder, version_id, verbose):
    native_applets = ["native_concat",
                      "native_diff",
                      "native_mk_list",
                      "native_sum",
                      "native_sum_012"]

    # build the native applets, only if they do not exist
    for napl in native_applets:
        applet = list(dxpy.bindings.search.find_data_objects(classname= "applet",
                                                             name= napl,
                                                             folder= applet_folder,
                                                             project= project.get_id()))
        if len(applet) == 0:
            cmdline = [ "dx", "build",
                        os.path.join(top_dir, "test/applets/{}".format(napl)),
                        "--destination", (project.get_id() + ":" + applet_folder + "/") ]
            print(" ".join(cmdline))
            subprocess.check_output(cmdline)

    native_one_call_dxni(project, applet_folder + "/native_concat", version_id, verbose)
    native_call_dxni(project, applet_folder, version_id, verbose)

def native_call_app_setup(version_id, verbose):
    app_name = "native_hello"

    # Check if they already exist
    apps = list(dxpy.bindings.search.find_apps(name= app_name))
    if len(apps) == 0:
        # build the app
        cmdline = [ "dx", "build", "--create-app", "--publish",
                    os.path.join(top_dir, "test/apps/{}".format(app_name)) ]
        print(" ".join(cmdline))
        subprocess.check_output(cmdline)

    # build WDL wrapper tasks in test/dx_extern.wdl
    header_file = os.path.join(top_dir, "test/wdl_1_0/dx_app_extern.wdl")
    cmdline = [ "java", "-jar",
                os.path.join(top_dir, "dxWDL-{}.jar".format(version_id)),
                "dxni",
                "--apps",
                "--force",
                "--language", "wdl_v1.0",
                "--output", header_file]
    if verbose:
        cmdline_common.append("--verbose")
    print(" ".join(cmdline))
    subprocess.check_output(cmdline)


######################################################################
# Compile the WDL files to dx:workflows and dx:applets
def compile_tests_to_project(trg_proj,
                             test_names,
                             applet_folder,
                             compiler_flags,
                             version_id,
                             lazy_flag):
    runnable = {}
    for tname in test_names:
        oid = None
        if lazy_flag:
            oid = lookup_dataobj(tname, trg_proj, applet_folder)
        if oid is None:
            c_flags = compiler_flags[:] + compiler_per_test_flags(tname)
            oid = build_test(tname, trg_proj, applet_folder, version_id, c_flags)
        runnable[tname] = oid
        print("runnable({}) = {}".format(tname, oid))
    return runnable


######################################################################
# Copy a workflow to an alternate project, and run it there.
def copy_wf_test(tname, src_proj, src_folder, debug_flag):
    alt_proj_name = "dxWDL_playground_2"
    trg_proj = util.get_project(alt_proj_name)
    if trg_proj is None:
        raise RuntimeError("Could not find project {}".format(alt_proj_name))
    test_folder = "/test"

    # clean up target
    print("cleaning up target")
    trg_proj.remove_folder(test_folder, recurse=True)
    trg_proj.new_folder(test_folder, parents=True)

    # copy to alternate project
    src_wf_id = lookup_dataobj(tname, src_proj, src_folder)
    print("copy {} to alternate project".format(src_wf_id))
    wf = dxpy.DXWorkflow(dxid=src_wf_id)
    wf2 = wf.clone(trg_proj.get_id(), folder=test_folder)

    # Run the workflow, and wait for completion
    runnable = {tname : wf2.get_id()}
    run_test_subset(trg_proj, runnable, test_folder, debug_flag)

######################################################################
## Program entry point
def main():
    global test_unlocked
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    argparser.add_argument("--archive", help="Archive old applets",
                           action="store_true", default=False)
    argparser.add_argument("--compile-only", help="Only compile the workflows, don't run them",
                           action="store_true", default=False)
    argparser.add_argument("--compile-mode", help="Compilation mode")
    argparser.add_argument("--debug", help="Run applets with debug-hold, and allow ssh",
                           action="store_true", default=False)
    argparser.add_argument("--force", help="Remove old versions of applets and workflows",
                           action="store_true", default=False)
    argparser.add_argument("--folder", help="Use an existing folder, instead of building dxWDL")
    argparser.add_argument("--lazy", help="Only compile workflows that are unbuilt",
                           action="store_true", default=False)
    argparser.add_argument("--list", "--test-list", help="Print a list of available tests",
                           action="store_true",
                           dest="test_list",
                           default=False)
    argparser.add_argument("--locked", help="Generate locked-down workflows",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAnexus project ID",
                           default="dxWDL_playground")
    argparser.add_argument("--stream-all-files", help="Stream all input files with dxfs2",
                           action="store_true", default=False)
    argparser.add_argument("--runtime-debug-level",
                           help="printing verbosity of task/workflow runner, {0,1,2}")
    argparser.add_argument("--test", help="Run a test, or a subgroup of tests",
                           action="append", default=[])
    argparser.add_argument("--unlocked", help="Generate only unlocked workflows",
                           action="store_true", default=False)
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
    argparser.add_argument("--verbose-key", help="Verbose compilation",
                           action="append", default=[])
    args = argparser.parse_args()

    print("top_dir={} test_dir={}".format(top_dir, test_dir))

    register_all_tests(args.verbose)
    if args.test_list:
        print_test_list()
        exit(0)
    test_names = []
    if len(args.test) == 0:
        args.test = 'M'
    for t in args.test:
        test_names += choose_tests(t)
    print("Running tests {}".format(test_names))
    version_id = util.get_version_id(top_dir)

    project = util.get_project(args.project)
    if project is None:
        raise RuntimeError("Could not find project {}".format(args.project))
    if args.folder is None:
        base_folder = util.build_dirs(project, version_id)
    else:
        # Use existing prebuilt folder
        base_folder = args.folder
    applet_folder = base_folder + "/applets"
    test_folder = base_folder + "/test"
    print("project: {} ({})".format(project.name, project.get_id()))
    print("folder: {}".format(base_folder))

    test_dict = {
        "aws:us-east-1" :  project.name + ":" + base_folder
    }

    # build the dxWDL jar file, only on us-east-1
    util.build(project, base_folder, version_id, top_dir, test_dict)

    if args.unlocked:
        # Disable all locked workflows
        args.locked = False
        test_unlocked = test_names

    compiler_flags = []
    if args.locked:
        compiler_flags.append("-locked")
        test_unlocked = []
    if args.archive:
        compiler_flags.append("-archive")
    if args.compile_mode:
        compiler_flags += ["-compileMode", args.compile_mode]
    if args.force:
        compiler_flags.append("-force")
    if args.verbose:
        compiler_flags.append("-verbose")
    if args.stream_all_files:
        compiler_flags.append("-streamAllFiles")
    if args.verbose_key:
        for key in args.verbose_key:
            compiler_flags += ["-verboseKey", key]
    if args.runtime_debug_level:
        compiler_flags += ["-runtimeDebugLevel", args.runtime_debug_level]

    #  is "native" included in one of the test names?
    if ("call_native" in test_names or
        "call_native_v1" in test_names):
        native_call_setup(project, applet_folder, version_id, args.verbose)
    if "call_native_app" in test_names:
        native_call_app_setup(version_id, args.verbose)

    try:
        # Compile the WDL files to dx:workflows and dx:applets
        runnable = compile_tests_to_project(project,
                                            test_names,
                                            applet_folder,
                                            compiler_flags,
                                            version_id,
                                            args.lazy)
        if not args.compile_only:
            run_test_subset(project, runnable, test_folder, args.debug)
    finally:
        print("Completed running tasks in {}".format(args.project))

if __name__ == '__main__':
    main()
