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

### Running unit tests

When submitting a PR, unit tests will be started automatically.

To invoke them via CLI access to the DNAnexus platform and a few internal projects is needed. First login into DNAnexus staging environment

```
dx login --staging
```

and then run the following commands from the dxWDL directory:

```
# select the dxWDL_playground project 
dx select dxWDL_playground

# run tests not marked as "prod" tests
sbt version && sbt compile && sbt "testOnly -- -l prod"
```

### Running test applications on DNAnexus

You can also run tests that first build and upload dxWDL assets to the DNAnexus platform and then compile test WDL code into applets. To run such tests, login to the platform (staging) and use `./scripts/run_tests.py `. For example, to execute a medium-sized subset of tests, run:

```
./scripts/run_tests.py --test M
```

You can also select a test to run from the [/test](/test) directory and invoke it by name, for example:

```
./scripts/run_tests.py --test add3
```

Check the test runner script `--help` for more options.
