version 1.0

# a workflow that has a required argument with a default.
# There was a bug (https://github.com/dnanexus/dxWDL/issues/284) where
# a call failed because it did not supplied [b].
workflow Foo {
    input {
        Int a
        Int b = 10
    }
    output {
        Int result = a + b
    }
}
