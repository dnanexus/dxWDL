version 1.0

struct Person {
  String name
  String city
  Int    age
}

workflow foo {
  input {
    String? city
  }

  # unary
  Int k0 = -1
  Int k1 = +1

  # place holders can only be tested in command blocks
  Array[String] cities = ["SF", "LA", "NYC"]

  Boolean b1 = true || false
  Boolean b2 = true && false
  Boolean b3 = 10 == 3
  Boolean b4 = 4 < 8
  Boolean b5 = 4 >= 8
  Boolean b6 = 4 != 8
  Boolean b7 = 4 <= 8
  Boolean b8 = 11 > 8

  # Arithmetic
  Int i1 = 3 + 4
  Int i2 = 3 - 4
  Int i3 = 3 % 4
  Int i4 = 3 * 4
  Int i5 = 3 / 4

  # array access
  Array[String] letters = ["a", "b", "c"]
  String l0 = letters[0]

  String l1 = if (false) then "a" else "b"

  # access pairs
  Pair[String, Boolean] p = ("hello", true)
  String l = p.left
  Boolean r = p.right

  Person pr1 = {
    "name" : "Jay",
    "city" : "SF",
    "age" : 31
  }

  String name = pr1.name
}
