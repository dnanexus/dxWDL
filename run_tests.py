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
test_input={}
test_output={}
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

# New interface, register a test name, and find its
# input file, and expected results file.
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
    test_files[wf_name] = desc
    desc

def register_test_fail(wf_name):
    register_test(wf_name)
    test_failing.add(wf_name)

######################################################################

# Read a JSON file
def read_json_file(path):
    print("Reading JSON file {}".format(path))
    with open(path, 'r') as fd:
        data = fd.read()
        d = json.loads(data)
        return d

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
            print("inputs={}".format(inputs))
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
def register_all_tests(project):
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
    #    register_test("gatk_170412",
#                  lambda x: gatk_gen_inputs(project, "H06HDADXX130110.1.ATCACGAT.20k_reads.bam"
#            find_bam_file("H06HDADXX130110.2.ATCACGAT.20k_reads.bam"),
#            find_bam_file("H06JUADXX130110.1.ATCACGAT.20k_reads.bam")),
#                  lambda x: {})
#    register_test("gatk_170412",
#                  lambda x: gatk_gen_inputs(project, [
#                      "NIST7035_TAAGGCGA_L001.bam",
#                      "NIST7035_TAAGGCGA_L002.bam",
#                      "NIST7086_CGTACTAG_L001.bam",
#                      "NIST7086_CGTACTAG_L002.bam"
#                  ]),
#                  lambda x: {})


######################################################################
### GATK testing

def find_file(name, folder, project):
    dxfile = dxpy.find_one_data_object(
        classname="file", name=name,
        project=project.get_id(), folder=folder,
        zero_ok=False, more_ok=False, return_handler=True)
    return dxpy.dxlink(dxfile.get_id(), project.get_id())


# The GATK pipeline takes many parameters, it is easier
# to treat it is a script.
#
# Adapted from:
# https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_160927.inputs.json
#
def gatk_gen_inputs(project, bam_files):
    def find_bam_file(name):
        return find_file(name, "/GATK_Compare", project)
    def find_interval_file(name, subfolder):
        return find_file(name,
                         "/genomics-public-data/resources/broad/hg38/v0/scattered_calling_intervals/" + subfolder + "/", project)
    def find_ref_file(name):
        return find_file(name, "/genomics-public-data/resources/broad/hg38/v0/", project)
    def find_qc_file(name):
        return find_file(name, "/genomics-public-data/test-data/qc", project)
    def find_intervals_file(name):
        return find_file(name, "/genomics-public-data/test-data/intervals", project)

    input_args = {
        ##_COMMENT1: SAMPLE NAME AND UNMAPPED BAMS
        "0.sample_name": "NIST-hg001-7001",
        "0.base_file_name": "NIST-hg001-7001-ready",
        "0.flowcell_unmapped_bams": [find_bam_file(bf) for bf in bam_files],
        "0.final_gvcf_name": "NIST-hg001-7001.g.vcf.gz",
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

        ## COMMENT2: REFERENCE FILES
        "0.ref_dict": find_ref_file("Homo_sapiens_assembly38.dict"),
        "0.ref_fasta": find_ref_file("Homo_sapiens_assembly38.fasta"),
        "0.ref_fasta_index": find_ref_file("Homo_sapiens_assembly38.fasta.fai"),
        "0.ref_alt": find_ref_file("Homo_sapiens_assembly38.fasta.64.alt"),
        "0.ref_sa": find_ref_file("Homo_sapiens_assembly38.fasta.64.sa"),
        "0.ref_amb": find_ref_file("Homo_sapiens_assembly38.fasta.64.amb"),
        "0.ref_bwt": find_ref_file("Homo_sapiens_assembly38.fasta.64.bwt"),
        "0.ref_ann": find_ref_file("Homo_sapiens_assembly38.fasta.64.ann"),
        "0.ref_pac": find_ref_file("Homo_sapiens_assembly38.fasta.64.pac"),

        ## COMMENT3: KNOWN SITES RESOURCES
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

        ##_COMMENT4: "QUALITY CONTROL RESOURCES
        "0.contamination_sites_vcf": find_qc_file("WholeGenomeShotgunContam.vcf"),
        "0.contamination_sites_vcf_index": find_qc_file("WholeGenomeShotgunContam.vcf.idx"),
        "0.haplotype_database_file": find_qc_file("empty.haplotype_map.txt"),
        "0.fingerprint_genotypes_file": find_qc_file("empty.fingerprint.vcf"),
        "0.wgs_coverage_interval_list": find_intervals_file("wgs_coverage_regions.hg38.interval_list"),
        "0.wgs_evaluation_interval_list": find_intervals_file("wgs_evaluation_regions.hg38.interval_list"),

        ## COMMENT5: QUALITY CONTROL SETTINGS (to override defaults)
#        "ValidateReadGroupSamFile.ignore": ["null"],
#        "ValidateReadGroupSamFile.max_output": 1000000000,
#        "ValidateAggregatedSamFile.ignore": ["null"],
#        "ValidateAggregatedSamFile.max_output": 1000000000,

        ## COMMENT5: DISK SIZES + PREEMPTIBLES
        "0.agg_small_disk": 200,
        "0.agg_medium_disk": 300,
        "0.agg_large_disk": 400,
        "0.agg_preemptible_tries": 3,
        "0.flowcell_small_disk": 100,
        "0.flowcell_medium_disk": 200,
        "0.preemptible_tries": 3
    }
    return input_args

######################################################################
# Program entry point
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
                           default="S")
    argparser.add_argument("--test-list", help="Print a list of available tests",
                           action="store_true", default=False)
    argparser.add_argument("--verbose", help="Verbose compilation",
                           action="store_true", default=False)
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
