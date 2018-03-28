# Nested scatter

workflow C {
    Array[String] streetNames = ["leghorn", "independence", "blossom"]
    Array[Int] numbers = [1, 2]

    scatter (sName in streetNames) {
        scatter (i in numbers) {
            String numberedStreets = sName + "_" + i
        }
    }
    output {
        numberedStreets
    }
}
