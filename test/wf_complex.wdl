workflow wf_complex {
    Object z = {"a": 3, "b": 1}

    output {
        Int sum = z.a + z.b
    }
}
