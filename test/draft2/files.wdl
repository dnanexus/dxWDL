# File path handling, and files with the same name

import "library_sys_call.wdl" as lib

workflow files {
    File f = "dx://dxWDL_playground:/test_data/fileB"
    File f1
    File f2
    File fruit_list
    Array[String] fruits_available = read_lines(fruit_list)

    # This isn't legal, because you can't stream
    # the same file twice.
    call lib.diff as diff_same {
        input: a=f, b=f
    }

    # Create an applet that ignores every input file
    call IgnoreAll { input: files = [] }

    # Try an applet that streams two files
    call lib.diff as diff1 {
        input: a=f, b=f1
    }

    call TsvGenTable
    call TsvReadTable { input : tbl_file = TsvGenTable.tbl }

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

    # Calculate the sizes of files
    scatter (x in FindFiles2.elements) {
        call lib.FileSize as FileSize {input: in_file=x}
    }

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

        call lib.FileIdent as ident {
          input:
             aF = sub(filename, prefix, "") + suffix,
             bF = sub(sub(filename, prefix, ""), prefix2, "") + suffix
        }
    }

    # Ragged array of files
    call FileArrayMake as mk1 {input: n=2}
    call FileArrayMake as mk2 {input: n=3}
    Array[Array[File]] allFiles = [mk1.result, mk2.result]
    scatter (fa in allFiles) {
        call lib.FileArraySize as FileArraySize {input: files=fa}
    }

    # scatter that calls a task that returns a file array
    scatter (k in [1,2,3]) {
        call FileArrayMake as mk_arr {input: n=k}
    }

    # conditionals
    if (2 < 1) {
        String false_branch = "This branch is not supposed to be taken"
    }
    if (length(allFiles) > 0) {
        call z_Copy as Copy3 { input : src=f, basename="branching" }
    }

   output {
       Int diff1_result = diff1.result
       Int diff2_result = diff2.result
       Array[File] FindFiles_texts = FindFiles.texts
       String colocation_result = colocation.result
       Array[Int] FileArraySize_result = FileArraySize.result
       Array[String] head_result = head.result
       Array[Array[String]] TsvReadTable_result = TsvReadTable.result
       Array[String] fruits = fruits_available
   }
}


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
        Array[Float] file_sizes = [size("A.txt"), size("B.txt"), size("C.txt")]
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

# Create the following TSV Table
# 11   12   13
# 21   22   23
task TsvGenTable {
    command {
        echo -e "11\t12\t13" > table.txt
        echo -e "21\t22\t23" >> table.txt
    }
    output {
        File tbl = "table.txt"
    }
}

# Read the table created by TsvGenTable.
# The task has an empty command section, so
# the [tbl_file] is supposted to be downloaded only
# in the output section (upon access).
task TsvReadTable {
    File tbl_file

    command {}
    output {
        Array[Array[String]] result = read_tsv(tbl_file)
    }
}

# Ignore all the input files
task IgnoreAll {
    Array[File] files
    command {}
    output {}
}
