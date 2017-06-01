# A very simple WDL file, that includes a single task
task ddd_join_many {
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

workflow var_types {
    Boolean b
    Int i
    Float x
    String s

    call ddd_join_many as join_many {
        input : b=b, i=i, x=x, s=s
    }
    output {
        join_many.result
    }
}
