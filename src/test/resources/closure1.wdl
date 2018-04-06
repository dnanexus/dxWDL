workflow closure1 {
    String wf_suffix = ".txt"

    scatter(filename in ["one", "two", "three", "four"]) {
        String prefix = ".txt"
        String suffix = wf_suffix

        call ident { input:
          aF = sub(filename, prefix, "") + suffix
        }
    }

    output {
        Array[File] results = ident.result
    }
}


task ident {
    File aF
    command {}
    output {
       File result = aF
    }
}
