task CountLines {
    File input_file

    command <<<
    wc -l ${input_file} | awk '{print $1}' > line.count
    >>>

    output {
        Int line_count = read_int("line.count")
    }
}

workflow CountLinesWorkflow {
    File input_file

    call CountLines {
        input: input_file=input_file
    }

    output {
        Int line_count=CountLines.line_count
    }
}
