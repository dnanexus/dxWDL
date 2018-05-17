task TB {
    String hotel
    String city
    String state

    command {}
    output {
        Array[String] retval = [hotel, city, state]
    }
}
