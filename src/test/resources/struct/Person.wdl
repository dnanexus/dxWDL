version 1.0

struct Person {
    String name
    Int age
}

task printPerson {
    input {
        Person a
    }
    command {
        echo "hello my name is ${a.name} and I am ${a.age} years old"
    }
    output {
        String result = read_string(stdout())
    }
}

workflow myWorkflow {
    input {
        Person a
    }
    call printPerson {
        input: a = a
    }

    output {
        String result = printPerson.result
    }
}
