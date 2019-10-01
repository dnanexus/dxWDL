workflow intermediate_files {
    call mk_file

    output {
        Int result = 3
    }
}


task mk_file {
    command {
        echo "giraffe and okapi are long necked mammals" > A.txt
    }
    output {
        File result = "A.txt"
    }
}
