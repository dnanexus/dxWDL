# check that we can use "last" in a variable name
workflow Hello {

    String last_name = "Jane"

    call SayHellastloPolitely {
        input:
                name = last_name
    }
    output {
        String output_string = SayHellastloPolitely.output_string
    }
}

task SayHellastloPolitely {
    String name

    command <<<
    echo "Hello, ${name}!"
    >>>

    output {
        String output_string  = "Nice to see you, ${name}!"
    }
}
