# Nested elements, no calls

workflow B {
    Array[String] fruit = ["apple", "cherry", "mango", "pear"]
    Array[Int] numbers = range(length(fruit))

    scatter (i in numbers) {
        Int square = i * i
        Boolean cond = i > 1
        if (cond) {
            String numberedFruit = fruit[i] + "_" + i
        }
    }
    output {
        square
        numberedFruit
    }
}
