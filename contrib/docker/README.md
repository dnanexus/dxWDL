# Building the Docker Container

The [Dockerfile](Dockerfile) in this folder will build a container to interact
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

```

## 3. Interactive Shell

You may want to log in and interact with DNAnexus. You can do that with an interactive
terminal (it) shell:

```bash
docker run -it --entrypoint bash dnanexus/dxwdl
```

**Where is the dxWDL?**

The jar is here, and this is how the container interacts with it when you run it:

```bash
$ java -jar /dxWDL-0.69.jar
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
