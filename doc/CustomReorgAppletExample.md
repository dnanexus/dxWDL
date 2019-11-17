

# Config file-based custom reorg applet example use case


Consider the following WDL task and workflow:

```
simple_wf {
    input {}
    call simple_task
    output {
      Array[File] out = simple_task.out
    }
}

simple_task {

  input {}

  command <<<
    touch out_1.vcf
    touch out.bam
  >>>

  output {
     Array[File] out= ["out.vcf", "out.bam"]
  }
}
```

The workflow will generate an array of files - `out.vcf` and `out.bam`.
Suppose we want to reorganize the results to 2 different folders on the project.

`out.vcf` will go to /results/out/vcf
`out.bam` will go to /bam/

A simple reorg applet is shown below.

## `___reorg_conf` as conf.json file.

The key is the suffix for the files that we will move to the destination declared in the value of the JSON object.

```
{
  "vcf": "/resuts/out/vcf",
  "bam": "/bam"
}

```

## code.py

The applet code does the following.

1) Read the configuration file.
2) Retrieve the list of output using the job and analysis id.
3) Move the files using dxpy.

```
import json

import dxpy

@dxpy.entry_point('main')
def main(___reorg_conf=None, ___reorg_status=None):

    # download and parse `___reorg_conf`
    conf_file = dxpy.DXFile(___reorg_conf)
    dxpy.download_dxfile(conf.get_id(), "conf.json")
    with open('conf.json') as f:
        conf = json.load(f)

    # find the output stage of the current analysis
    analysis_id = dxpy.describe(dxpy.JOB_ID)["analysis"]
    stages = dxpy.describe(analysis_id)["stages"]

    # retrieve the dictionary containing outputs, where key is the name of output and value is the link to the file.
    output_map = [x['execution']['output'] for x in stages if x['id'] == 'stage-outputs'][0]

    out = output_map['out']

    bam = [x for x in out if x.endswith('.bam')][0]
    vcf = [x for x in out if x.endswith('.vcf')][0]

    vcf_folder = conf['vcf']
    bam_folder = conf['bam']

    # get the container instance
    dx_container = dxpy.DXProject(dxpy.PROJECT_CONTEXT_ID)

    dx_container.move(
        destination=vcf_folder,
        objects=[ vcf['$dnanexus_link'] ]
    )
    dx_container.move(
        objects=bam_folder,
        destination=[ bam['$dnanexus_link'] ],
    )

```

## dxapp.json

This is the spec for the applet.
`___reorg_conf` is declared in `custom-reorg.conf` in extras.json (JSON file provided to `-extras`).

```
{
  "name": "custom_reorg_app",
  "title": "Example custom reorg app",
  "summary": "A small example to show how to use the config file based custom reorg app",
  "dxapi": "1.0.0",
  "inputSpec": [
    {
      "name": "___reorg_conf",
      "label": "Config",
      "help": "",
      "class": "file",
      "patterns": ["*"],
      "optional": true
    },
    {
      "name": "___reorg_status",
      "label": "Config",
      "help": "",
      "class": "string",
      "optional": true
    }
  ],
  "runSpec": {
    "interpreter": "python2.7",
    "timeoutPolicy": {
      "*": {
        "hours": 48
      }
    },
    "distribution": "Ubuntu",
    "release": "16.04",
    "file": "code.py"
  },
  "access": {
    "network": [
      "*"
    ],
    "project": "CONTRIBUTE"
  },
  "ignoreReuse": false,
  "regionalOptions": {
    "aws:us-east-1": {
      "systemRequirements": {
        "*": {
          "instanceType": "mem1_ssd1_x4"
        }
      }
    }
  }
}
```

## `extras.json` (JSON file provided to `-extras`).

```
{
  "custom-reorg" : {
    "app_id" : "applet-12345678910",
    "conf" : "dx://file-xxxxxxxx"
  }
}

```
