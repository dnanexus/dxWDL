version 1.0

struct Person {
  String name
  Int    height
  Int    age
}

struct House {
  String street
  String city
  Int    zipcode
}

workflow census {
  Person p1 = {
    "name" : "Carly",
    "height" : 168,
    "age" : 40
  }

  output {
    Person p = p1
  }
}
