version 1.0

# A library of individual tasks. The plan is to import this file into
#the workflows. This will avoid creating duplicate applets, and reduces compilation
#time.
task Add {
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

task Multiply {
    input {
        Int a
        Int b
    }
    command {
        echo $((a * b))
    }
    output {
        Int result = a * b
    }
}

task Inc {
    input {
        Int i
    }
    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task Sum {
    input {
        Array[Int] ints
    }
    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}


# Create an array of integers from an integer.
task RangeFromInt {
    input {
        Int len
    }
    command {}
    output {
        Array[Int] result = range(len)
    }
}

# checking behavior with empty arrays
task ArrayLength {
    input {
        Array[Int] ai
    }
    command {}
    output {
        Int result = length(ai)
    }
}
