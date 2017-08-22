# File path handling, and files with the same name

import "library_sys_call.wdl" as lib

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

task z_FindFiles {
    command <<<
      echo "ABCD" > A.txt
      echo "XYZW" > Y.txt
      mkdir -p H
      echo "Marriot" > H/M.hotel
      echo "Ritz" > H/R.hotel
    >>>
    output {
        Array[File] texts = glob("*.txt")
        Array[File] hotels = glob("H/*.hotel")
    }
}

# This glob returns an empty array. It causes
# an input/output error in dx.
task z_FindFiles2 {
    command <<<
      echo "Iron" > A.txt
      echo "Silver" > B.txt
      echo "Gold" > C.txt
    >>>
    output {
        Array[File] elements = glob('*.txt')
        Array[File] emptyFiles = glob('*.none')
    }
}

task z_FileSizes {
    File car_desc

    command <<<
      echo "Iron" > A.txt
      echo "Silver" > B.txt
      echo "Gold" > C.txt
    >>>
    output {
      Array[Float] file_sizes = [size("A.txt"), size("B.txt"), size("C.txt")]
      Float car_size = size(car_desc)
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

task z_file_ident {
    File fileA
    File fileB

    command {
    }
    output {
      String result = fileA
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

# Calculate the total number of bytes the array has
task FileArraySize {
    Array[File] files

    parameter_meta {
        files : "stream"
    }
    command <<<
        wc -c ${sep=' ' files} | cut -d ' ' -f 1 | tail -1
    >>>
    output {
        Int result = read_int(stdout())
    }
}

workflow files {
    File f
    File f1
    File f2

    # Try an applet that streams two files
    call lib.diff as diff1 {
        input: a=f, b=f
    }
    call lib.diff as diff2 {
        input: a=f, b=f2
    }

    call lib.Colocation as colocation {
        input : A=f1, B=f2
    }

    call z_Copy as Copy { input : src=f, basename="tearFrog" }
    call z_Copy as Copy2 { input : src=Copy.outf, basename="mixing" }
    call z_FindFiles as FindFiles
    call z_FindFiles2 as FindFiles2
    call z_FileSizes as FileSizes { input: car_desc=FindFiles.texts[0] }

    String wf_suffix = ".txt"

    scatter (x in ["one", "two", "three", "four"]) {
        call GenFile {input: str=x}
        call lib.wc as wc {input: in_file = GenFile.out}
        call lib.head as head {input: in_file = GenFile.out, num_lines=1}
    }

    scatter (filename in GenFile.out) {
        String prefix = ".txt"
        String prefix2 = ".cpp"
        String suffix = wf_suffix

        call z_file_ident as ident {
          input:
             fileA = sub(filename, prefix, "") + suffix,
             fileB = sub(sub(filename, prefix, ""), prefix2, "") + suffix
        }
    }

    # Ragged array of files
    call FileArrayMake as mk1 {input: n=2}
    call FileArrayMake as mk2 {input: n=3}
    Array[Array[File]] allFiles = [mk1.result, mk2.result]
    scatter (fa in allFiles) {
        call FileArraySize {input: files=fa}
    }

    # conditionals
    if (2 < 1) {
        String false_branch = "This branch is not supposed to be taken"
    }
    if (length(allFiles) > 0) {
        call z_Copy as Copy3 { input : src=f, basename="branching" }
    }

    output {
        diff1.result
        diff2.result
        Copy2.outf_sorted
        FindFiles.texts
        FindFiles.hotels
#       FindFiles2.elements
#       FindFiles2.emptyFiles
        colocation.result
        ident.result
        FileArraySize.result
        false_branch
        Copy3.outf
        head.result
    }
}
