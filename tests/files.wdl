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

workflow files {
    File f

    call z_Copy as Copy { input : src=f, basename="tearFrog" }
    call z_Copy as Copy2 { input : src=Copy.outf, basename="mixing" }

    output {
       Copy2.outf_sorted
    }
}