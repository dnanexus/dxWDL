version 1.0

struct Tree {
  String kind
  String state
  Int height
  Int age
}


workflow foo {
  Boolean b1 = true
  Boolean b2 = b1

  Int i1 = 13
  Int i2 = 13.1
  Int i3 = "8"

  Float x1 = 3
  Float x2 = i1
  Float x3 = 44.3
  Float x4 = x3
  Float x5 = "4.5"

  String s1 = true
  String s2 = 3
  String s3 = 4.3
  String s4 = "hello"
  String? s5 = s4

  Object fir = object {
    kind : "Fir",
    state : "Colorado",
    height : 187,
    age : 403
  }

  Tree fir2 = fir
}
