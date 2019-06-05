version 1.0

task B {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b + 1
    }
}
