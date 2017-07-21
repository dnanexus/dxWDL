# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes and README
- Make sure the version number in `reference.conf` is correct
- Merge onto master branch, make sure travis tests pass
- Tag git with the release version
- Build new externally visible release
  * run `build_jar.py --release`
  * Update [releases](https://github.com/dnanexus-rnd/dxWDL/releases) github page,
    use the `Draft a new release` button.

## Post release
- Update the version number in `reference.conf`. We don't want
to mix the experimental release, with the old code.


## Coding guidelines

- Lines should not go over 100 characters
- Unit tests should assert, and not print to the console
- WDL test files belong in the top directory `test`
