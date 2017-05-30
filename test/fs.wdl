# A very simple WDL file, that includes a single task
task cat {
    File inputFile

    command {
        cat ${inputFile} > B.txt
    }
    output {
        File result = "B.txt"
    }
}

workflow fs {
    File data

    call cat {
        input : inputFile=data
    }
    output {
        cat.result
    }
}
