version 1.0

workflow wf_constants {
    input {
        File f = "dx://dxWDL_playground:/test_data/fileB"
        File g = "dx://dxWDL_playground:/test_data/fileC"
        File fruit_list = "dx://dxWDL_playground:/test_data/fruit_list.txt"

        Int i = 43
    }

    output {
        File fo = f
        File go = g

        Int io = i + 10
    }
}
