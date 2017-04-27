# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes
- Update version string in `Utils.scala`
- Run Travis tests by cloning into the `citest` branch.
- Merge onto master branch
- Tag the git with the release version
- Build new externally visible release
