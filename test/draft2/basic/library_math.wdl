# A library of individual tasks. The plan is to import this file into
#the workflows. This will avoid creating duplicate applets, and reduces compilation
#time.
task Add {
    Int a
    Int b

    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}

task Multiply {
    Int a
    Int b

    command {
        echo $((a * b))
    }
    output {
        Int result = a * b
    }
}

task Inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task Sum {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task Twice {
    Int i

    command <<<
        python -c "print(${i} * 2)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task Mod7 {
    Int i

    command <<<
        python -c "print(${i} % 7)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task IntOps {
    Int a
    Int b

    command {
    }
    output {
        Int mul = a * b
        Int sum = a + b
        Int sub = a - b
        Int div = a / b
        Int ai = a
        Int bi = b
        Int result = a * b + 1
    }
}


# Create an array of integers from an integer.
task RangeFromInt {
    Int len
    command {}
    output {
        Array[Int] result = range(len)
    }
}

# checking behavior with empty arrays
task ArrayLength {
    Array[Int] ai
    command {}
    output {
        Int result = length(ai)
    }
}

task MaybeInt {
    Int? a
    command {
    }
    output {
        Int? result = a
    }
}
