# Developing dxWDL

## Setting up a Development Environment

### Developing in a Docker container

A Dockerfile with all the dependencies to build and test dxWDL is at [/development/docker/Dockerfile](docker/Dockerfile). To build an image from it and run a Docker container, run from the dxWDL repo root folder:

```
make -f development/Makefile
```

To recompile dxWDL with your updates, run a test, as described [here](#Running-tests).

> Always make sure you push your changes to the remote github repo before destroying your Docker container.

## Running tests

First login into DNAnexus staging environment

```
dx login --staging
```

Then run a test from the repo root folder. To run a subset of tests, run:

```
./scripts/run_tests.py --test M
```

You can also select a test to run from [/test](/test) and invoke it by name, for example:

```
./scripts/run_tests.py --test add3
```

Check test runner script help for more options.


