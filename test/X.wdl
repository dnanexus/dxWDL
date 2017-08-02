# File path handling, and files with the same name

# Trying out file copy operations
task z_Copy {
    File src
    String basename

    command <<<
        cp ${src} ${basename}.txt
        sort ${src} > ${basename}.sorted.txt
    >>>
    output {
      File outf = "${basename}.txt"
      File outf_sorted = "${basename}.sorted.txt"
    }
}

# create a ragged array of files, and count how many bytes each file-row
# takes.
# Create an array of [NR] files
task FileArrayMake{
    Int n

    command <<<
       for i in `seq ${n}`
       do
          echo $i > $i.txt
       done
    >>>
    output {
        Array[File] result = glob("*.txt")
    }
}

workflow X {
    # Ragged array of files
    call FileArrayMake as mk1 {input: n=2}
    call FileArrayMake as mk2 {input: n=3}
    Array[Array[File]] allFiles = [mk1.result, mk2.result]

    # conditionals
    if (2 < 1) {
        String false_branch = "This branch is not supposed to be taken"
    }
    if (length(allFiles) > 0) {
        call z_Copy as Copy3 { input : src="/tmp/X.txt", basename="branching" }
    }

    output {
        false_branch
        Copy3.outf
    }
}
