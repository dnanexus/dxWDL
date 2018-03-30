# Nested elements, no calls

workflow B {
    Array[String] fruit = ["apple", "cherry", "mango", "pear"]

    scatter (i in range(length(fruit))) {
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
