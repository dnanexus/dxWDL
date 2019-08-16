version 1.0

task Add {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}

task Multiply {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a * b
    }
}

task Concat {
    input {
        String a
        String b
    }
    command {}
    output {
        String result = a + "_" + b
    }
}

task NoArguments {
    input {
        String a = "goodbye"
    }
    command {}
    output {
        String result = a + " " + "and thank you for the fish"
    }
}
