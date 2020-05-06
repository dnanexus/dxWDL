version 1.0

task comparisons {
  input {
    Int i
    Boolean b
    Float x
    File f
    String s
  }

  # comparisons of the same type
  Boolean ib1 = i == i
  Boolean ib2 = i != i
  Boolean ib3 = i < i
  Boolean ib4 = i > i
  Boolean ib5 = i >= i
  Boolean ib6 = i <= i

  # You can even compare booleans
  Boolean bb1 = b == b
  Boolean bb2 = b != b
  Boolean bb3 = b < b
  Boolean bb4 = b > b
  Boolean bb5 = b >= b
  Boolean bb6 = b <= b

  Boolean xb1 = x == x
  Boolean xb2 = x != x
  Boolean xb3 = x < x
  Boolean xb4 = x > x
  Boolean xb5 = x >= x
  Boolean xb6 = x <= x

  Boolean fb1 = f == f
  Boolean fb2 = f != f
  Boolean fb3 = f < f
  Boolean fb4 = f > f
  Boolean fb5 = f >= f
  Boolean fb6 = f <= f

  Boolean sb1 = s == s
  Boolean sb2 = s != s
  Boolean sb3 = s < s
  Boolean sb4 = s > s
  Boolean sb5 = s >= s
  Boolean sb6 = s <= s

  # comparisons between types
  Boolean cb1 = x < i
  Boolean cb2 = i > x


  # compound types
  Boolean ct_1 = [1, 2] == [1, 3, 4]
  Boolean ct_2 = ["1", "2"] == ["3"]
  Boolean ct_3 = [[1], [2]] == [[1], [3], [4]]

  command {}
}
