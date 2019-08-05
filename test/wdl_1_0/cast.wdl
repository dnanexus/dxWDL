version 1.0

task AddV1 {
    input {
        Int a
        Int b
    }
    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}

task EmptyArray {
    input {
        Array[Int] fooAr
    }
    command {
    }
    output {
        Array[Int] result=fooAr
    }
}


task SumArray {
    input {
        Array[Int] numbers
    }
    command <<<
        python -c "print(~{sep=" + " numbers})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task JoinMisc {
    input {
        Boolean b
        Int i
        Float x
        String s
    }
    command {
    }
    output {
        String result = "${b}_${i}_${x}_${s}"
    }
}

workflow cast {
    input {
        Int i
        String s
        File foo
    }
    Array[Int] iArr = [i]
    Array[String] sArr = [s]
    Array[File] fooArr = [foo]

    # Handling of empty arrays as input/output
    call EmptyArray { input: fooAr=[] }

    call AddV1 as Add { input: a=i, b=i }
    call SumArray {input: numbers=iArr }

    call SumArray as SumArray2 {input: numbers=[i] }

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
