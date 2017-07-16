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

workflow files {
    File f

    call z_Copy as Copy { input : src=f, basename="tearFrog" }
    call z_Copy as Copy2 { input : src=Copy.outf, basename="mixing" }
    call z_FindFiles as FindFiles
    call z_FindFiles2 as FindFiles2
    call z_FileSizes as FileSizes { input: car_desc=FindFiles.texts[0] }

    output {
       Copy2.outf_sorted
       FindFiles.texts
       FindFiles.hotels
#       FindFiles2.elements
#       FindFiles2.emptyFiles
    }
}
