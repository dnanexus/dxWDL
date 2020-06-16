# A sub-block with a conditional
import "library_math.wdl" as lib

workflow conditionals2 {
    Boolean flag
    Int n = 7

    if (flag) {
        call lib.z_add as add { input: a=5, b=1, n=n }
        call lib.z_add as add2 {input: a=3, b=2, n=n }
        call genFile { input: str="Jabberwocky" }
    }

    File lewis_carol = select_first([genFile.summary])

    scatter (i in [1, 3, 5]) {
        Int k = select_first([add2.result])
        call lib.z_mul as mulcall {input: a=i, b=k, n=n}
    }

    output {
        Int? add2_r = add2.result
        Array[Int] mul = mulcall.result
        File lc = lewis_carol
    }
}

# Create a file with several lines.
task genFile {
    String str
    command <<<
       echo "Nut" >> ${str}.txt
       echo "Screwdriver" >> ${str}.txt
       echo "Wrench" >> ${str}.txt
    >>>
    output {
        File summary = "${str}.txt"
    }
}
