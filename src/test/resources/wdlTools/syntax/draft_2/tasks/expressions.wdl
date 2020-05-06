# These are various expressions
task district {
  Int i = 3
  String s = "hello world"
  Float x = 4.3
  File f = "/dummy/x.txt"
  Boolean b = false

  # Logical expressions
  Boolean b2 = true || false
  Boolean b3 = true && false
  Boolean b4 = 3 == 5
  Boolean b5 = 4 < 5
  Boolean b6 = 4 >= 5
  Boolean b7 = 6 != 7
  Boolean b8 = 6 <= 7
  Boolean b9 = 6 > 7
  Boolean b10 = !b2

  # Arithmetic
  Int j = 4 + 5
  Int j1 = 4 % 10
  Int j2 = 10 / 7
  Int j3 = j
  Int j4 = j + 19

  Array[Int] ia = [1, 2, 3]
  Array[Int]+ ia = [10]
  Int k = ia[3]
  Int k2 = f(1, 2, 3)
  Map[Int, String] m = {1 : "a", 2: "b"}
  Int k3 = if (true) then 1 else 2
  Int k4 = x.a
  Object o = object { A : 1, B : 2 }
  Pair[Int, String] twenty_threes = (23, "twenty-three")

  command{}
}
