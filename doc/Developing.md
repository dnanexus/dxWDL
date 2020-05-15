# Developing dxWDL

## Setting up your development environment

* Install JDK 8
  * On mac with [homebrew]() installed:
    ```
    $ brew tap AdoptOpenJDK/openjdk
    $ brew cask install adoptopenjdk8
    # Use java_home to find the location of JAVA_HOME to set
    $ /usr/libexec/java_home -V
    $ export JAVA_HOME=/Library/Java/...
    ```
  * On Linux (assuming Ubuntu 16.04)
    ```
    $ sudo apt install openjdk-8-jre-headless
    ```
  * Scala will compile with JDK 11, but the JDK on DNAnexus worker instances is JDK 8 and will not be able to run a JAR file with classes compiled by a later version of Java
* Install [sbt](https://www.scala-sbt.org/), which also installs Scala. Sbt is a make-like utility that works with the ```scala``` language.
  * On MacOS: `brew install sbt`
  * On Linux:
    ```
    $ wget www.scala-lang.org/files/archive/scala-2.12.1.deb
    $ sudo dpkg -i scala-2.12.1.deb
    $ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    $ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    $ sudo apt-get update
    $ sudo apt-get install sbt
    ```
  * Running sbt for the first time takes several minutes, because it
downloads all required packages.
* We also recommend to install [Metals](https://scalameta.org/metals/), which enables better integration with your IDE
  * For VSCode, install the "Scala (Metals)" and "Scala Syntax (official)" plugins
* You will also need Python installed (we recommend version 3.6+) and dx-toolkit (`pip install dxpy`)

## Getting the source code

* Clone or fork the [dxWDL repository](https://github.com/dnanexus/dxWDL) (depending on whether you have commit permissions)
* Checkout an existing branch or create a new branch (e.g. feat/42-my-feature)
* Add pre-commit hooks:
  * Create/edit a file .git/hooks/pre-commit
  * Add the following lines
    ```bash
    #!/bin/bash
    check=$(sbt scalafmtCheckAll)
    if [[ "$?" -ne "0" ]]; then
      echo "Reformatting; please commit again"
      sbt scalafmtAll
      exit $check
    fi
    ```
  * Make the file executable (e.g. `chmod +x .git/hooks/pre-commit`)
  * This hook runs the code formatter before committing code. You can run this command manually, but it is easiest just to have it run automatically.

## Adding new code

* Follow the style guidelines (below).
* Always write unit tests for any new code you add, and update tests for any code you modify.
  * Unit tests should assert, and not print to the console
  * WDL test files belong in the top directory `test`
* Submit a pull request when you are ready to have your code reviewed.

### Style guidelines

* We use [scalafmt style](https://scalameta.org/scalafmt/) with a few modifications. You don't need to worry so much about code style since you will use the automatic formatter on your code before it is committed.
* Readability is more important than efficiency or concision - write more/slower code if it makes the code more readable.
* Avoid using more complex features, e.g. reflection.

## Using sbt

sbt is the build system for Scala (similar to a Makefile). The following are the main commands you will use while developing.

### Compiling the code

Scala (like Java) is a compiled language. To compile, run:

```
$ sbt compile
```

If there are errors in your code, the compiler will fail with (hopefully useful) error messages.

### Running unit tests

You should always run the unit tests after every successful compile. Generally, you want to run `sbt testQuick`, which only runs the tests that failed previously, as well as the tests for any code you've modified since the last time you ran the tests. However, the first time you checkout the code (to make sure your development environment is set up correctly) and then right before you push any changes to the repository, you should run the full test suite using `sbt test`.

You need to have a DNAnexus account and be logged into DNAnexus via the command line before you can run the tests (`dx login`).

### Running the integration tests

Integration tests actually build and run apps/workflows on DNAnexus. These tests take much longer to run than the unit tests, and so typically you only run them before submitting a pull request.

First, you need to an account on the DNAnexus staging environment, and you need to be added to the projects that have been setup for testing; @orodeh can set this up for you. Next, log into the DNAnexus staging environment using dx-toolkit: `dx login --staging`. Note that very often your login will timeout while the integration tests are running unless you are actively using the platform in another session, and this will cause the tests to fail. To avoid, this, generate a token via the web UI and use that token to log in on the command line: `dx login --staging --token <token>`.

Next, delete any existing build artifacts using the following commands:

```
$ sbt clean
$ sbt cleanFiles
$ find . -name target | xargs rm -rf
```

You may also need to delete artifiacts that have been cached on the platform:

```
$ dx rm -r dxWDL_playground:/builds/<username>/<version>
```

Finally, run the integration tests. From the root dxWDL directory, run `./scripts/run_tests.py`.

Note that the test script does a lot of things for you. If for some reason you want to run them manually, here is what happens:

* TODO: fill out this list
* The dxWDL JAR file is built and staged in the root dxWDL directory. To do this manually, run `sbt assembly`, then move the JAR file from the `target` folder to the root dxWDL folder, e.g. `mv target/dxWDL.jar ./dxWDL-1.44.jar`.

### Other sbt tips

#### Cache

sbt keeps the cache of downloaded jar files in `${HOME}/.ivy2/cache`. For example, the WDL jar files are under `${HOME}/.ivy2/cache/org.broadinstitute`. In case of problems with cached jars, you can remove this directory recursively. This will make WDL download all dependencies (again).

## Releasing a new version

### Release check list

- Make sure regression tests pass
- Update release notes and README.md
- Make sure the version number in `src/main/resources/application.conf` is correct. It is used
when building the release.
- Merge onto master branch, and make sure [travis tests](https://travis-ci.org/dnanexus/dxWDL) pass
- Clean your `dx` environment because you'll be using limited-power tokens to run the release script. Do not
mix them with your regular user token.
```
dx clearenv
```
- Build new externally visible release
  ```
  ./scripts/build_all_releases.sh --staging-token XXX --production-token YYY --docker-user UUU --docker-password WWW
  ```
  this will take a while. It builds the release on staging, runs multi-region tests on staging, builds on production, and creates an easy to use docker image.
- Update [releases](https://github.com/dnanexus-rnd/dxWDL/releases) github page, use the `Draft a new release` button, and upload a dxWDL.jar file.

### Post release

- Update the version number in `src/main/resources/application.conf`. We don't want
to mix the experimental release, with the old code.
