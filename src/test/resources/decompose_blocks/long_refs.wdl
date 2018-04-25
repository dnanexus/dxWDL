workflow long_refs {
    String wf_suffix = ".txt"
    scatter (x in ["one", "two", "three", "four"]) {
        call GenFile {input: str=x}
        call wc as wc {input: in_file = GenFile.out}
        call head as head {input: in_file = GenFile.out, num_lines=1}
    }

    scatter (filename in GenFile.out) {
        String prefix = ".txt"
        String prefix2 = ".cpp"
        String suffix = wf_suffix

        call FileIdent {
          input:
             aF = sub(filename, prefix, "") + suffix
        }
    }

    output {
        Array[File] out = GenFile.out
        Array[String] result = head.result
        Array[Int] line_count = wc.line_count
        Array[File] result2 = FileIdent.result
    }
}

# Create a file with several lines.
task GenFile {
    String str
    command <<<
       echo "Nut" >> ${str}.txt
       echo "Screwdriver" >> ${str}.txt
       echo "Wrench" >> ${str}.txt
    >>>
    output {
        File out = "${str}.txt"
    }
}

task wc {
    File in_file

    parameter_meta {
        in_file : "stream"
    }
    command <<<
    wc -l ${in_file} | awk '{print $1}' > line.count
    >>>

    output {
        Int line_count = read_int("line.count")
    }
}


task head {
    File in_file
    Int num_lines

    parameter_meta {
        in_file : "stream"
    }
    command {
        head -n ${num_lines} ${in_file}
    }
    output {
        String result = read_string(stdout())
    }
}

task FileIdent {
    File aF

    command {
    }
    output {
       File result = aF
    }
}
