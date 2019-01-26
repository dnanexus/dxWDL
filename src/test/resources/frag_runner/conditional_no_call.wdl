version 1.0

workflow conditional_no_call {
    Boolean flag = true
    if (flag) {
        String cats = "Mr. Baggins"
    }
    output {
        String? result = cats
    }
}
