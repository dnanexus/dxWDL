version 1.0

struct Person {
    String name
    Int age
}

task printPerson {
    input {
    }
    Person a = object {
      name: "John",
      age: 30
    }

    command {
        echo "hello my name is ${a.name} and I am ${a.age} years old"
    }
    output {
        String result = read_string(stdout())
    }
}
