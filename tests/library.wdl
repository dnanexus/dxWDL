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
