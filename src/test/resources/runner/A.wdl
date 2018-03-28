workflow A {
    Array[Int] numbers = [1, 2, 3]
    scatter (i in numbers) {
        Int k = i * 2
    }
    Boolean alwaysTrue = true
    if (alwaysTrue) {
        String s = "hello class of 2017"
    }
    output {
        k
        s
    }
}
