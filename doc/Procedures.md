# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes and README
- Update version number in `reference.conf`
- Merge onto master branch, make sure travis tests pass
- Tag git with the release version
- Build new externally visible release
  * run `release.py`
  * Update [releases](https://github.com/dnanexus-rnd/dxWDL/releases) github page,
    use the `Draft a new release` button.
