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

# Create a complex number with a WDL object
task ComplexGen {
    Int a
    Int b

    command <<<
python <<CODE
print('\t'.join(["a", "b"]))
print('\t'.join(["${a}", "${b}"]))
CODE
>>>
    output {
        Object result = read_object(stdout())
    }
}

# Add to complex numbers represented as objects
task ComplexAdd {
    Object y
    Object z

    command <<<
python <<CODE
a = int(${y.a}) + int(${z.a})
b = int(${y.b}) + int(${z.b})
print('\t'.join(["a","b"]))
print('\t'.join([str(a), str(b)]))
CODE
>>>
    output {
        Object result = read_object(stdout())
    }
}
