workflow intermediate_files {

    scatter (x in [1, 2, 3]) {
        call mk_file
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
