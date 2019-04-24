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

workflow wf_person {
    input {
    }

    Person a = {
        "name": "John",
        "age": 30
    }
    call printPerson {
        input: a = a
    }

    output {
        String result = printPerson.result
    }
}
