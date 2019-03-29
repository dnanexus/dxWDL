import "library_math.wdl" as lib

# With arithmetic in the [mod n] additive group, calculate:
# [a, 2a, 3a, ..., n*a]
workflow array_add {
    Int n
    Int a

    scatter (i in range(n)) {
        call lib.z_add as add {
            input: n=n, a=a, b=i
        }
    }

    call bug_nop {
        input: x=n
    }
    output {
        Array[Int] result = add.result
    }
}

task bug_nop {
    Int x

    command {
        echo "Hello, world!"
    }
}
