workflow A {
    scatter (i in [1, 2, 3]) {
        Int k = i * 2
    }
    if (true) {
        String s = "hello class of 2017"
    }
    output {
        k
        s
    }
}
