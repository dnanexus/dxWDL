# Building the Docker Container

The [Dockerfile](..Dockerfile) in the root folder will build a container to interact
with the tools in this repository.

## 1. Build the Container
From the folder with the Dockerfile:

```bash
docker build -t dnanexus/dxwdl .
```

## 2. Run the container

```bash
$ docker run vanessa/dx-toolkit
java -jar dxWDL.jar <action> <parameters> [options]

Actions:
  compile <WDL file>
    Compile a wdl file into a dnanexus workflow.
    Optionally, specify a destination path on the
    platform. If a WDL inputs files is specified, a dx JSON
    inputs file is generated from it.
    options
      -archive               Archive older versions of applets
      -compileMode <string>  Compilation mode, a debugging flag
      -defaults <string>     File with Cromwell formatted default values (JSON)
      -destination <string>  Output path on the platform for workflow
      -destination_unicode <string>  destination in unicode encoded as hexadecimal
      -extras <string>       JSON formatted file with extra options, for example
                             default runtime options for tasks.
      -inputs <string>       File with Cromwell formatted inputs
      -locked                Create a locked-down workflow
      -p | -imports <string> Directory to search for imported WDL files
      -reorg                 Reorganize workflow output files
      -runtimeDebugLevel [0,1,2] How much debug information to write to the
                             job log at runtime. Zero means write the minimum,
                             one is the default, and two is for internal debugging.

  dxni
    Dx Native call Interface. Create stubs for calling dx
    executables (apps/applets/workflows), and store them as WDL
    tasks in a local file. Allows calling existing platform executables
    without modification. Default is to look for applets.
    options:
      -apps                  Search only for global apps.
      -o <string>            Destination file for WDL task definitions
      -r | recursive         Recursive search

Common options
    -destination             Full platform path (project:/folder)
    -f | force               Delete existing applets/workflows
    -folder <string>         Platform folder
    -project <string>        Platform project
    -quiet                   Do not print warnings or informational outputs
    -verbose [flag]          Print detailed progress reports

```

## 3. Interact with dxWDL.jar

You most likely want to use the container to convert (or otherwise interact with)
some file on your host. Let's say I have a WDL file in the present working directory,
and I want to use the container to convert it. I would need to bind the $PWD to somewhere
in the container (note that /opt is empty) and then output there as well to see
the new file on the host. It might look like this:

```bash
WDL=chip.wdl
docker run -v $PWD:/opt dnanexus/dxwdl compile $WDL ... # (continued)
```

## 4. Work Interactively
You may want to log in and interact with DNAnexus. You can do that with an interactive
terminal (it) shell:

```bash
docker run -it --entrypoint bash dnanexus/dxwdl
```

**Where is the dxWDL?**

The jar is here, and this is how the container interacts with it when you run it:

```bash
$ java -jar /dxWDL-0.69.jar


Actions:
  compile <WDL file>
    Compile a wdl file into a dnanexus workflow.
    Optionally, specify a destination path on the
    platform. If a WDL inputs files is specified, a dx JSON
    inputs file is generated from it.
    options
      -archive               Archive older versions of applets
      -compileMode <string>  Compilation mode, a debugging flag
      -defaults <string>     File with Cromwell formatted default values (JSON)
      -destination <string>  Output path on the platform for workflow
      -destination_unicode <string>  destination in unicode encoded as hexadecimal
      -extras <string>       JSON formatted file with extra options, for example
                             default runtime options for tasks.
      -inputs <string>       File with Cromwell formatted inputs
      -locked                Create a locked-down workflow
      -p | -imports <string> Directory to search for imported WDL files
      -reorg                 Reorganize workflow output files
      -runtimeDebugLevel [0,1,2] How much debug information to write to the
                             job log at runtime. Zero means write the minimum,
                             one is the default, and two is for internal debugging.

...
```

**Login to DNAnexus**

root@b144d75a1d0e:/# dx login
Acquiring credentials from https://auth.dnanexus.com
Username: myusername
Password: 

Note: Use dx select --level VIEW or dx select --public to select from projects for
which you only have VIEW permissions.

Available projects (CONTRIBUTE or higher):
0) CHIP-SEQ-PIPELINE (CONTRIBUTE)
1) WGBS-PIPELINE (CONTRIBUTE)
2) ATAC-SEQ-PIPELINE (CONTRIBUTE)
3) HIC-PIPELINE (CONTRIBUTE)

Pick a numbered choice: 0
Setting current project to: CHIP-SEQ-PIPELINE
You are now logged in. Your credentials are stored in /root/.dnanexus_config and will expire in 30
days, 0:00:00. Use dx login --timeout to control the expiration date, or dx logout
to end this session.
```

Of course once you are logged in, do **not** push or otherwise share that particular container!
And remember that when you exit the session, it will end.


