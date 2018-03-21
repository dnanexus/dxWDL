import "library_math.wdl" as lib
import "library_sys_call.wdl" as lib_file

task EmptyArray {
     Array[Int] fooAr

     command {
     }
     output {
       Array[Int] result=fooAr
     }
}


task SumArray {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task JoinMisc {
    Boolean b
    Int i
    Float x
    String s

    command {
    }
    output {
        String result = "${b}_${i}_${x}_${s}"
    }
}

workflow cast {
    Int i
    String s
    File foo
    Array[Int] iArr = [i]
    Array[String] sArr = [s]
    Array[File] fooArr = [foo]

    # Handling of empty arrays as input/output
    call EmptyArray { input: fooAr=[] }

    call lib_file.FileIdent as FileIdent { input: aF = foo }
    File foo2 = FileIdent.result
    Array[File] fArr2 = [foo2]

    call lib.Add as Add { input: a=i, b=i }
    call SumArray {input: ints=iArr }

    call SumArray as SumArray2 {input: ints=[i] }

    # Check various rarely used types (float, boolean)
    Boolean b = true
    Int i2 = "3"
    Float x = "4.2"
    String s2 = "zoology"
    call JoinMisc {
        input : b=b, i=i2, x=x, s=s2
    }

    output {
        Int Add_result = Add.result
        Int SumArray_result = SumArray.result
        Int SumArray2_result = SumArray2.result
        String JoinMisc_result = JoinMisc.result
    }
}
