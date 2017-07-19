# File path handling, and files with the same name

import "library_sys_call.wdl" as lib
import "library_sg.wdl" as lib_sg

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

task z_analysis {
    String str
    command <<<
       echo "xyz12345" >> ${str}.txt
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


workflow files {
    File f
    File f1
    File f2

    call lib.Colocation as colocation {
        input : A=f1, B=f2
    }

    call z_Copy as Copy { input : src=f, basename="tearFrog" }
    call z_Copy as Copy2 { input : src=Copy.outf, basename="mixing" }
    call z_FindFiles as FindFiles
    call z_FindFiles2 as FindFiles2
    call z_FileSizes as FileSizes { input: car_desc=FindFiles.texts[0] }

    String wf_suffix = ".txt"

    call lib_sg.Prepare as prepare
    scatter (x in prepare.array) {
        call z_analysis as analysis {input: str=x}
    }
    call lib_sg.Gather as gather {input: files=analysis.out}

    scatter (filename in analysis.out) {
        String prefix = ".txt"
        String prefix2 = ".cpp"
        String suffix = wf_suffix

        call z_file_ident as ident {
          input:
             fileA = sub(filename, prefix, "") + suffix,
             fileB = sub(sub(filename, prefix, ""), prefix2, "") + suffix
        }
    }
    output {
        Copy2.outf_sorted
        FindFiles.texts
        FindFiles.hotels
#       FindFiles2.elements
#       FindFiles2.emptyFiles
        colocation.result
        gather.str
        ident.result
    }
}
