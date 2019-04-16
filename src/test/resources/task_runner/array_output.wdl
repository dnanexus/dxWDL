task array_output {
    String hotel
    String city
    String state

    command {}
    output {
        Array[String] retval = [hotel, city, state]
    }
}
