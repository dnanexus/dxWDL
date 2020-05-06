version 1.0

struct Tree {
  String kind
  String state
  Int height
  Int age
}


workflow foo {
  # missing fields
  Tree badTree = object {
    kind : "Fir",
    state : "Colorado"
  }
}
