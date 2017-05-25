# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
# This one is a variant that is has a forward reference
# First and very simple test of topological sorting
task add3_Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int sum = a + b
    }
}


# Topo ordering add3_Add, Add3, scatter { Add3NestedScatter, Add3Scatter, Add3DependentScatter } , Add3Final
workflow add3 {
    Int ai
    Int bi
    Int ci
    Array[Int] xs = [1,2,3]

    scatter (x in xs) {
        call add3_Add as Add3DependentScatter { input: a = x, b = Add3Scatter.sum }
        call add3_Add as Add3Scatter { input: a = x, b = Add3More.sum }
        scatter (y in xs) {
            call add3_Add as Add3NestedScatter { input: a = ai, b = Add3.sum }
        }
    }

    scatter (x in xs) {
        call add3_Add as LinkScatter {input: a = Add3NestedScatter.sum, b = bi}
    }

    call add3_Add { input: a = ai, b = bi }
    call add3_Add as Add3 { input: a = add3_Add.sum, b = ci }
    call add3_Add as Add3Final { input: a = Add3More.sum, b = Add3More.sum }
    call add3_Add as Add3More { input: a = Add3.sum, b = add3_Add.sum }
    call add3_Add as OutsideScatter { input: a=Add3Scatter.sum, b=bi }

}
