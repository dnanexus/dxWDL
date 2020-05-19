# Developing dxWDL

## Setting up a Development Environment

### Developing in a Docker container

A Dockerfile with all the dependencies to build and test dxWDL is available [here](docker/Dockerfile). To build an image from it and run a Docker container, run:

```
make
```

To recompile dxWDL with your updates, run a test, as described [here](#Running-tests).

> Always make sure you push your changes to the remote github repo before destroying your Docker container.

## Running tests

First login into DNAnexus staging environment

```
dx login --staging
```

Then run a test from the dxWDL repository root folder. To run a medium-sized subset of tests, run:

```
./scripts/run_tests.py --test M
```

You can also select a test to run from the [/test](/test) directory and invoke it by name, for example:

```
./scripts/run_tests.py --test add3
```

Check the test runner script `--help` for more options.

