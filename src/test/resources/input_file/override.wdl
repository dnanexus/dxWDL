version 1.0

workflow override {
    call etl { input: a = 3 }
    output {
        Int result = etl.result
    }
}

task etl {
    input {
        Int a
        Int b = 10
        Int? c
    }
    command {}
    output {
        Int result = a + b
    }
}
