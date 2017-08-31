# Check that rarely used types actually work (float, boolean).
# Experiment with casting between types.

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
        # Casting
        Int num17 = "17"
        Float f1 = "1.2"

        String jm = join_many.result
    }
}
