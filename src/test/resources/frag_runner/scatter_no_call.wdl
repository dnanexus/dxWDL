version 1.0

workflow scatter_no_call {
    Array[String] names = ["Michael", "Lukas", "Martin", "Shelly", "Amy"]
    scatter (x in names) {
        String full_name = x + "_Manhaim"
    }
    output {
        Array[String] result = full_name
    }
}
