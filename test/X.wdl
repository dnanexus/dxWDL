import "library_math.wdl" as lib

workflow X {
    Int x
    Int y
    Pair[Float,Int] p = (1.0, 19)

    output {
        # Expressions in output section
        Int expr1 = x + y
        Int expr2 = x + expr1
        Float z = p.left
    }
}
