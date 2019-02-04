version 1.0

import "library_math.wdl" as lib
import "library_string.wdl" as lib_str

workflow check_imports {
    # accessing members of a pair structure
    Pair[Int, Int] p2 = (5, 8)
    call lib.Multiply as mul {
        input: a=p2.left, b=p2.right
    }

    Pair[String, String] v = ("carrots", "oranges")
    Pair[String, String] f = ("pear", "coconut")
    call lib_str.Concat as concat {
        input: x=v.left, y=f.right
    }

    output {
        Int r_mul = mul.result
        String r_con = concat.result
    }
}
