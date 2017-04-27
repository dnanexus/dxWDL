#!/usr/bin/env python
from __future__ import print_function

import argparse
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
git_revision = subprocess.check_output(["git", "describe", "--always", "--dirty", "--tags"]).strip()
git_revision_in_jar= subprocess.check_output(["git", "describe", "--always", "--tags"]).strip()
test_input={}
test_output={}
test_failing=set([])
reserved_test_names=['S', 'M', 'All', 'list']
default_test_list = [
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
    "bad_status", "bad_status2" ]

tmp_files=[]
dxfile = None
dxfile2 = None
dxfile3 = None
dxfile_v2 = None

def main():
    argparser = argparse.ArgumentParser(description="Run WDL compiler tests on the platform")
    argparser.add_argument("--project", help="DNAnexus project ID", default="project-F07pBj80ZvgfzQK28j35Gj54")
    argparser.add_argument("--no-wait", help="Exit immediately after launching tests",
                           action="store_true", default=False)
    argparser.add_argument("--compile-only", help="Only compile the workflows, don't run them",
                           action="store_true", default=False)
    argparser.add_argument("--compile-mode", help="Compilation mode")
    argparser.add_argument("--lazy", help="Only compile workflows that are unbuilt",
                           action="store_true", default=False)
    argparser.add_argument("--test", help="Run a test, or a subgroup of tests",
                           default="M")
    argparser.add_argument("--test-list", help="Print a list of available tests",
                           action="store_true", default=False)
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
    argparser.add_argument("--folder", help="Use an existing folder, instead of building dxWDL")
    args = argparser.parse_args()

    project = dxpy.DXProject(args.project)
    register_all_tests(project)
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
    print("project: {} ({})".format(project.name, args.project))
    print("folder: {}".format(base_folder))

    compiler_flags=[]
    if args.verbose:
        compiler_flags.append("--verbose")
    if args.compile_mode:
        compiler_flags += ["--mode", args.compile_mode]

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
            run_workflow_subset(project, workflows, test_folder, args.no_wait)
    finally:
        cleanup(project)


def run_workflow_subset(project, workflows, test_folder, no_wait):
    create_tmp_files(project)

    # Run the workflows
    test_analyses=[]
    for wf_name, dxid in workflows.iteritems():
        print("Generating inputs for {}".format(wf_name))
        inputs = test_input[wf_name]("dummy")
        print("Running workflow {}".format(wf_name))
        test_job = run_workflow(project, test_folder, wf_name, dxid, inputs)
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
        output = desc["output"]
        shouldbe = test_output[wf_name]("dummy")
        correct = True
        print("Checking results for workflow {}".format(wf_name))

        for key, expected_val in shouldbe.iteritems():
            correct = validate_result(wf_name, desc, key, expected_val)
        if correct:
            print("Analysis {} passed".format(wf_name))


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
            print("Should be stage[{}].{} = {} != {}".format(stage_name, field_name, result, expected_val))
            return False
        return True
    except Exception, e:
        #print("no stage {} in results {}".format(stage_name, desc['stages']))
        print("exception message={}".format(e))
        return False

def cleanup(project):
    if len(tmp_files) > 0:
        project.remove_folder("/test_data", recurse=True, force=True)

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
    test_dir = "tests"
    print("Compiling {}.wdl to a workflow".format(wf_name))
    cmdline = [ (top_dir + "/dxWDL"),
                "compile",
                os.path.join(top_dir, test_dir, wf_name + ".wdl"),
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


# Run [workflow] on several inputs. Return a tuple with three elements:
#  - job-ids of running analysis
#  - names of result folders
#  - expected results
def run_workflow(project, test_folder, wf_name, wfId, inputs):
    workflow = dxpy.DXWorkflow(project=project.get_id(), dxid=wfId)
    test_folder = os.path.join(test_folder, wf_name)
    project.new_folder(test_folder, parents=True)
    job = workflow.run(inputs,
                       project=project.get_id(),
                       folder=test_folder,
                       name="{} {}".format(wf_name, git_revision),
                       instance_type="mem1_ssd1_x2")
    return job


def print_test_list():
    l = [key for key in test_input.keys()]
    l.sort()
    ls = "\n  ".join(l)
    print("List of tests:\n  {}".format(ls))

# Choose set set of tests to run
def choose_tests(test_name):
    if test_name == 'M':
        return default_test_list
    if test_name == 'All':
        return test_input.keys()
    if test_name in test_input.keys():
        return [test_name]
    # Last chance: check if the name is a prefix.
    # Accept it if there is exactly a single match.
    matches = [key for key in test_input.keys() if key.startswith(test_name)]
    if len(matches) > 1:
        raise Exception("Too many matches for test prefix {} -> {}".format(test_name, matches))
    if len(matches) == 0:
        raise Exception("Test prefix {} is unknown".format(test_name))
    return matches

# Register inputs and outputs for all the tests.
# Return the list of temporary files on the platform
def register_all_tests(project):
    register_test("math",
                  lambda x: {'0.ai' : 7},
                  lambda x: {'Multiply.result': 40})
    register_test("system_calls",
                  lambda x: {'0.data' : dxpy.dxlink(dxfile.get_id(), project.get_id()),
                             '0.pattern' : "WDL"},
                  lambda x: {'cgrep.count' : 5, 'wc.count' : 14})
    register_test("four_step",
                  lambda x: { '0.ai' : 1, '0.bi' : 1},
                  lambda x: {'Add4.sum' : 16})
    register_test("add3",
                  lambda x: { '0.ai' : 1, '0.bi' : 2, '0.ci' : 5 },
                  lambda x: {'Add3.sum' : 8})
    register_test("concat",
                  lambda x: { "0.s1": "Yellow", "0.s2": "Submarine" },
                  lambda x: {'join2.result' : "Yellow_Submarine"})

    # There is a bug in marshaling floats. Hopefully,
    # it will get fixed in future wdl4s releases.
    register_test("var_types",
                  lambda x: {"0.b": True, "0.i": 3, "0.x": 4.2, "0.s": "zoology"},
                  lambda x: {})
#                  {'0.result' : "true_3_4.2_zoology"})

    register_test("fs",
                  lambda x: {'0.data' : dxpy.dxlink(dxfile.get_id(), project.get_id())},
                  lambda x: {})
    register_test("system_calls2",
                  lambda x: {'0.pattern' : "java"},
                  lambda x: {})
    register_test("math_expr",
                  lambda x: {'0.ai' : 2, '0.bi' : 3},
                  lambda x: {'int_ops1.mul' : 6,
                             'int_ops1.sum' : 5,
                             'int_ops1.sub' : -1,
                             'int_ops1.div' : 0,
                             'int_ops2.mul' : 36,
                             'int_ops2.div' : 1,
                             'int_ops3.sum' : 14,
                             'int_ops3.mul' : 40})
    register_test("string_expr",
                  lambda x: {},
                  lambda x: {'string_ops.result' :
                             "delicate.aligned__number.duplicate_metrics__xRIPx_toads_salamander"})
    register_test("call_expressions",
                  lambda x: {'0.i1' : 1, '0.i2' : 2},
                  lambda x: {'int_ops2.result': 25})
    register_test("call_expressions2",
                  lambda x: {'0.i' : 3, '0.s' : "frogs"},
                  lambda x: {'string_ops.result' : "frogs.aligned__frogs.duplicate_metrics__xRIPx",
                             'int_ops.result': 37,
                             'int_ops2.result': 7031})

    register_test("files",
                  lambda x: {'0.f' : dxpy.dxlink(dxfile.get_id(), project.get_id()) },
                  lambda x: {})
    register_test("string_array",
                  lambda x: { '0.sa' : ["A", "B", "C"]},
                  lambda x: { 'Concat.result' : "A INPUT=B INPUT=C"})

    register_test("file_array",
                  lambda x: { '0.fs' : [dxpy.dxlink(dxfile.get_id(), project.get_id()),
                                        dxpy.dxlink(dxfile2.get_id(), project.get_id()),
                                        dxpy.dxlink(dxfile3.get_id(), project.get_id()) ]},
                  lambda x: { 'colocation.result' : "True"})
    register_test("output_array",
                  lambda x: {},
                  lambda x: {'prepare.array' : [u'one', u'two', u'three', u'four']})
    register_test("file_disambiguation",
                  lambda x: { '0.f1' : dxpy.dxlink(dxfile.get_id(), project.get_id()),
                              '0.f2' : dxpy.dxlink(dxfile_v2.get_id(), project.get_id()) },
                  lambda x: { 'colocation.result' : "False"})

    # Scatter/gather
    register_test("sg_sum",
                  lambda x: {'0.integers' :    [1,2,3,4,5] },
                  lambda x: { 'sum.sum' : 20 })
    register_test("sg1",
                  lambda x: {},
                  lambda x: {'gather.str': "_one_ _two_ _three_ _four_"})
    register_test("sg_sum2",
                  lambda x: {'0.integers' : [1,8,11] },
                  lambda x: {'sum.sum' : 11 })
    register_test("sg_sum3",
                  lambda x: {'0.integers' : [2,3,5] },
                  lambda x: {'sum.sum' : 15 })
    register_test("sg_files",
                  lambda x: {},
                  lambda x: {})

    # ragged arrays
    register_test("ragged_array",
                  lambda x: {},
                  lambda x: {'processTsv.result' : "1\n2\n3\t4\n5\n6\t7\t8"})
    register_test("ragged_array2",
                  lambda x: {},
                  lambda x: {'collect.result':  ["1", "2", "3 INPUT=4", "5", "6 INPUT=7 INPUT=8"]})

    # optionals
    register_test("optionals",
                  lambda x: { "0.arg1": 10, "mul2.i": 5, "add.a" : 1, "add.b" : 3},
                  lambda x: { "mul2.result" : 10, "add.result" : 4 })

    # docker
    register_test("bwa_version",
                  lambda x: {},
                  lambda x: {'GetBwaVersion.version' : "0.7.13-r1126"})
    register_test_fail("bad_status",
                       lambda x: {},
                       lambda x: {})
    register_test_fail("bad_status2",
                       lambda x: {},
                       lambda x: {})

    # Output error
    register_test_fail("missing_output",
                       lambda x: {},
                       lambda x: {})

    # combination of featuers
    register_test("advanced",
                  lambda x: { '0.pattern' : "github",
                              '0.file' : dxpy.dxlink(dxfile.get_id(), project.get_id()),
                              '0.species' : "Arctic fox" },
                  lambda x: { 'str_animals.result' : "Arctic fox --K -S --flags --contamination 0 --s foobar",
                              'str_animals.family' : "Family Arctic fox",
                              #                    '2.cgrep___count': [6, 0, 6] }
                              })
    register_test("decl_mid_wf",
                  lambda x: {'0.s': "Yellow", '0.i': 4},
                  lambda x: {"add.sum": 15,
                             "concat.result": "Yellow.aligned_Yellow.wgs",
                             "add2.sum": 24})

    # Massive tests
    register_test("gatk_160927",
                  lambda x: gatk_gen_inputs(project),
                  lambda x: {})


def create_tmp_files(project):
    global tmp_files
    global dxfile
    global dxfile2
    global dxfile3
    global dxfile_v2
    if len(tmp_files) > 0:
        return tmp_files

    buf = """
Right now this is just a stripped-down version of
[wdltool](https://github.com/broadinstitute/wdltool) which we're using
as a playground to learn the ways of
[wdl4s](https://github.com/broadinstitute/wdl4s). It contains one new
subcommand to parse a WDL file and transcode the resulting AST into
YAML. That's just an exercise to familiarize ourselves with the WDL
syntax and wdl4s object hierarchy.

Useful links:
* [tutorial WDLs](https://github.com/broadinstitute/wdl/tree/develop/scripts/tutorials/wdl)
* [GATK production WDLs](https://github.com/broadinstitute/wdl/tree/develop/scripts/broad_pipelines)
* [WDL spec](https://github.com/broadinstitute/wdl/blob/develop/SPEC.md)
* [wdl4s scaladoc](http://broadinstitute.github.io/wdl4s/0.6/#package)
    """

    buf2 = """
Website and User Guide
The WDL website is the best place to go for more information on both WDL and Cromwell. In particular new users should check out the user guide which has many tutorials, examples and other bits to get you started.
"""

    buf3 = """
Building
sbt assembly will build a runnable JAR in target/scala-2.11/

Tests are run via sbt test. Note that the tests do require Docker to be running. To test this out while downloading the Ubuntu image that is required for tests, run docker pull ubuntu:latest prior to running sbt test
"""

    try:
        project.new_folder("/test_data")
    except:
        pass
    dxfile = dxpy.upload_string(buf, project=project.get_id(), name="fileA",
                                folder="/test_data")
    dxfile2 = dxpy.upload_string(buf2, project=project.get_id(), name="fileB",
                                 folder="/test_data")
    dxfile3 = dxpy.upload_string(buf3, project=project.get_id(), name="fileC",
                                 folder="/test_data")
    dxfile_v2 = dxpy.upload_string("ABCD 1234", project=project.get_id(), name="fileA",
                                   folder="/test_data")
    tmp_files = [dxfile, dxfile2, dxfile3, dxfile_v2]
    return tmp_files


# The GATK pipeline takes many parameters, it is easier
# to treat it is a script.
#
# Adapted from:
# https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_160927.inputs.json
#
def gatk_gen_inputs(project):
    def find_file(name, folder):
        dxfile = dxpy.find_one_data_object(
            classname="file", name=name,
            project=project.get_id(), folder=folder,
            zero_ok=False, more_ok=False, return_handler=True)
        return dxpy.dxlink(dxfile.get_id(), project.get_id())
    def find_bam_file(name):
        return find_file(name,
                         "/genomics-public-data/test-data/dna/wgs/hiseq2500/NA12878/")
    def find_interval_file(name, subfolder):
        return find_file(name,
                         "/genomics-public-data/resources/broad/hg38/v0/scattered_calling_intervals/" + subfolder + "/")
    def find_ref_file(name):
        return find_file(name,
                         "/genomics-public-data/resources/broad/hg38/v0/")

    input_args = {
        ## COMMENT1: SAMPLE NAME AND UNMAPPED BAMS
        "0.sample_name": "NA12878",
        "0.flowcell_unmapped_bams": [
            find_bam_file("H06HDADXX130110.1.ATCACGAT.20k_reads.bam"),
            find_bam_file("H06HDADXX130110.2.ATCACGAT.20k_reads.bam"),
            find_bam_file("H06JUADXX130110.1.ATCACGAT.20k_reads.bam")
        ],
        "0.final_gvcf_name": "NA12878.g.vcf.gz",
        "0.unmapped_bam_suffix": ".bam",

        ## COMMENT2: INTERVALS
        "0.scattered_calling_intervals": [
            find_interval_file("scattered.interval_list", "temp_0001_of_50"),
            find_interval_file("scattered.interval_list", "temp_0002_of_50"),
            find_interval_file("scattered.interval_list", "temp_0003_of_50"),
            find_interval_file("scattered.interval_list", "temp_0004_of_50"),
            find_interval_file("scattered.interval_list", "temp_0005_of_50"),
            find_interval_file("scattered.interval_list", "temp_0006_of_50"),
            find_interval_file("scattered.interval_list", "temp_0007_of_50"),
            find_interval_file("scattered.interval_list", "temp_0008_of_50"),
            find_interval_file("scattered.interval_list", "temp_0009_of_50"),
            find_interval_file("scattered.interval_list", "temp_0010_of_50"),
            find_interval_file("scattered.interval_list", "temp_0011_of_50"),
            find_interval_file("scattered.interval_list", "temp_0012_of_50"),
            find_interval_file("scattered.interval_list", "temp_0013_of_50"),
            find_interval_file("scattered.interval_list", "temp_0014_of_50"),
            find_interval_file("scattered.interval_list", "temp_0015_of_50"),
            find_interval_file("scattered.interval_list", "temp_0016_of_50"),
            find_interval_file("scattered.interval_list", "temp_0017_of_50"),
            find_interval_file("scattered.interval_list", "temp_0018_of_50"),
            find_interval_file("scattered.interval_list", "temp_0019_of_50"),
            find_interval_file("scattered.interval_list", "temp_0020_of_50"),
            find_interval_file("scattered.interval_list", "temp_0021_of_50"),
            find_interval_file("scattered.interval_list", "temp_0022_of_50"),
            find_interval_file("scattered.interval_list", "temp_0023_of_50"),
            find_interval_file("scattered.interval_list", "temp_0024_of_50"),
            find_interval_file("scattered.interval_list", "temp_0025_of_50"),
            find_interval_file("scattered.interval_list", "temp_0026_of_50"),
            find_interval_file("scattered.interval_list", "temp_0027_of_50"),
            find_interval_file("scattered.interval_list", "temp_0028_of_50"),
            find_interval_file("scattered.interval_list", "temp_0029_of_50"),
            find_interval_file("scattered.interval_list", "temp_0030_of_50"),
            find_interval_file("scattered.interval_list", "temp_0031_of_50"),
            find_interval_file("scattered.interval_list", "temp_0032_of_50"),
            find_interval_file("scattered.interval_list", "temp_0033_of_50"),
            find_interval_file("scattered.interval_list", "temp_0034_of_50"),
            find_interval_file("scattered.interval_list", "temp_0035_of_50"),
            find_interval_file("scattered.interval_list", "temp_0036_of_50"),
            find_interval_file("scattered.interval_list", "temp_0037_of_50"),
            find_interval_file("scattered.interval_list", "temp_0038_of_50"),
            find_interval_file("scattered.interval_list", "temp_0039_of_50"),
            find_interval_file("scattered.interval_list", "temp_0040_of_50"),
            find_interval_file("scattered.interval_list", "temp_0041_of_50"),
            find_interval_file("scattered.interval_list", "temp_0042_of_50"),
            find_interval_file("scattered.interval_list", "temp_0043_of_50"),
            find_interval_file("scattered.interval_list", "temp_0044_of_50"),
            find_interval_file("scattered.interval_list", "temp_0045_of_50"),
            find_interval_file("scattered.interval_list", "temp_0046_of_50"),
            find_interval_file("scattered.interval_list", "temp_0047_of_50"),
            find_interval_file("scattered.interval_list", "temp_0048_of_50"),
            find_interval_file("scattered.interval_list", "temp_0049_of_50"),
            find_interval_file("scattered.interval_list", "temp_0050_of_50")
        ],
        "0.wgs_calling_interval_list": find_ref_file("wgs_calling_regions.hg38.interval_list"),

        ## COMMENT2: OPTIONAL ARGUMENTS
        #"0.HaplotypeCaller.contamination": 0,

        ## COMMENT3: REFERENCE FILES
        "0.ref_dict": find_ref_file("Homo_sapiens_assembly38.dict"),
        "0.ref_fasta": find_ref_file("Homo_sapiens_assembly38.fasta"),
        "0.ref_fasta_index": find_ref_file("Homo_sapiens_assembly38.fasta.fai"),
        "0.ref_alt": find_ref_file("Homo_sapiens_assembly38.fasta.64.alt"),
        "0.ref_sa": find_ref_file("Homo_sapiens_assembly38.fasta.64.sa"),
        "0.ref_amb": find_ref_file("Homo_sapiens_assembly38.fasta.64.amb"),
        "0.ref_bwt": find_ref_file("Homo_sapiens_assembly38.fasta.64.bwt"),
        "0.ref_ann": find_ref_file("Homo_sapiens_assembly38.fasta.64.ann"),
        "0.ref_pac": find_ref_file("Homo_sapiens_assembly38.fasta.64.pac"),

        ## COMMENT4: KNOWN SITES RESOURCES
        "0.dbSNP_vcf": find_ref_file("Homo_sapiens_assembly38.dbsnp138.vcf"),
        "0.dbSNP_vcf_index": find_ref_file("Homo_sapiens_assembly38.dbsnp138.vcf.idx"),
        "0.known_indels_sites_VCFs": [
            find_ref_file("Mills_and_1000G_gold_standard.indels.hg38.vcf.gz"),
            find_ref_file("Homo_sapiens_assembly38.known_indels.vcf.gz")
        ],
        "0.known_indels_sites_indices": [
            find_ref_file("Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi"),
            find_ref_file("Homo_sapiens_assembly38.known_indels.vcf.gz.tbi")
        ],

        ## COMMENT5: DISK SIZES + PREEMPTIBLES
        "0.agg_small_disk": 200,
        "0.agg_medium_disk": 300,
        "0.agg_large_disk": 400,
        "0.agg_preemptible_tries": 3,
        "0.flowcell_small_disk": 200,
        "0.flowcell_medium_disk": 300,
        "0.preemptible_tries": 3
    }
    return input_args

def register_test(wf_name, gen_inputs, gen_outputs):
    if wf_name in reserved_test_names:
        raise Exception("Test name {} is reserved".format(wf_name))
    test_input[wf_name] = gen_inputs
    test_output[wf_name] = gen_outputs

def register_test_fail(wf_name, inputs, outputs):
    register_test(wf_name, inputs, outputs)
    test_failing.add(wf_name)

if __name__ == '__main__':
    main()
