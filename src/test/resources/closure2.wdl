workflow closure2 {

    # Ragged array of strings
    call createTsv
    scatter (x in createTsv.result) {
        call processLine as processLine {input : line=x}
    }
}


# Generate a ragged string array
task createTsv {
    command {}
    output {
      Array[Array[String]] result = [["A", "B"], ["C"], ["D", "E", "F"]]
    }
}

task processLine {
    String line
    command {}
    output {
        String out = line
    }
}
