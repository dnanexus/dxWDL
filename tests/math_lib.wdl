# A library of individual tasks. The plan is to import this file into
#the workflows. This will avoid creating duplicate applets, and reduces compilation
#time.
task Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
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
