# Nested scatter

workflow C {
    scatter (sName in ["leghorn", "independence", "blossom"]) {
        scatter (i in [1, 2]) {
            String numberedStreets = sName + "_" + i
        }
    }
    output {
        numberedStreets
    }
}
