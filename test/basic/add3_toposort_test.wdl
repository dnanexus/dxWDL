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

task processIntMatrix {
    Array[Array[Int]] intMatrix
    Int x
    command { echo "hello" }
}

task processIntArray {
    Array[Int] intArray
    Int x
    command { echo "oink" }
}



# Topo ordering add3_Add, Add3, scatter { Add3NestedScatter, Add3Scatter, Add3DependentScatter } , Add3Final
workflow add3 {
    Int ai
    Int bi
    Int ci

    scatter (x in xs) {
        call add3_Add as Add3DependentScatter { input: a = x, b = Add3Scatter.sum }
        call add3_Add as Add3Scatter { input: a = x, b = Add3More.sum }
        scatter (y in xs) {
            call add3_Add as Add3NestedScatter { input: a = ai, b = Add3.sum }
        }
    }

    Array[Int] xs = [1,2,3]

    scatter (x in xs) {
        call processIntMatrix {input: intMatrix = Add3NestedScatter.sum, x = x}
    }

    call add3_Add as Add3 { input: a = add3_Add.sum, b = ci }
    call add3_Add as Add3Final { input: a = Add3More.sum, b = Add3More.sum }
    call add3_Add as Add3More { input: a = Add3.sum, b = add3_Add.sum }
    call add3_Add { input: a = ai, b = bi }
    call processIntArray { input: intArray=Add3Scatter.sum, x=bi }

}
