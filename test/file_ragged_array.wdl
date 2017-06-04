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

# Calculate the total number of bytes the array has
task FileArraySize {
    Array[File] files

    command <<<
        wc -c ${sep=' ' files} | cut -d ' ' -f 1 | tail -1
    >>>
    output {
        Int result = read_int(stdout())
    }
}

workflow file_ragged_array {
    call FileArrayMake as mk1 {input: n=2}
    call FileArrayMake as mk2 {input: n=3}

    Array[Array[File]] allFiles = [mk1.result, mk2.result]
    scatter (fa in allFiles) {
        call FileArraySize {input: files=fa}
    }
    output {
        FileArraySize.result
    }
}