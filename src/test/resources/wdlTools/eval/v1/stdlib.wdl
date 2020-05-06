version 1.0

struct House {
  String city
  String team
  Int zipcode
}

workflow foo {
  input {
    String? empty_string
  }

  Float x = 1.4
  Int n1 = floor(x)
  Int n2 = ceil(x)
  Int n3 = round(x)

  Array[String] cities = ["LA", "Seattle", "San Francisco"]
  File flc = write_lines(cities)
  Array[String] cities2 = read_lines(flc)

  Array[Array[String]] table = [
  ["A", "allow"],
  ["B", "big"],
  ["C", "clam"]]

  File flTbl = write_tsv(table)
  Array[Array[String]] table2 = read_tsv(flTbl)

  Map[String, String] m = {"name" : "hawk", "kind" : "bird" }
  File flm = write_map(m)
  Map[String, String] m2 = read_map(flm)

  # sub
  String sentence = "He visited three places on his trip: Aa, Ab, C, D, and E"
  String sentence1 = sub(sentence, "He", "She")
  String sentence2 = sub(sentence, "A[ab]", "Berlin")
  String sentence3 = sub(sentence, "[a-z]*", "")

  # range
  Array[Int] ar3 = range(3)

  # transpose
  Array[Array[Int]] ar_ar = [[1, 2, 3], [4, 5, 6]]
  Array[Array[Int]] ar_ar2 = transpose(ar_ar)

  # zip
  Array[String] letters = ["A", "B", "C"]
  Array[Boolean] flags = [true, false, true]
  Array[Pair[String, Boolean]] zlf = zip(letters, flags)

  # cross
  Array[Int] numbers = [1, 13]
  Array[Pair[String, Int]] cln = cross(letters, numbers)

  # length
  Int l1 = length(numbers)
  Int l2 = length(cln)

  # flatten
  Array[Array[File]] files = [["A", "B", "C"], ["G"], ["J", "K"]]
  Array[File] files2 = flatten(files)

  # prefix
  Array[Int] xx_numbers = [1, 3, 5, 7]
  Array[String] pref2 = prefix("i_", xx_numbers)
  Array[Float] xx_float = [1.0, 3.4, 5.1]
  Array[String] pref3 = prefix("sub_", xx_float)

  # select_first
  String sel1 = select_first(["A", "B"])
  String? name = "Henry"
  String sel2 = select_first([empty_string, name])

  # select_all
  Array[String] sel3 = select_all([empty_string, name, "bear", "tree"])

  Boolean d1 = defined(name)
  Boolean d2 = defined(empty_string)
  Boolean d3 = defined(xx_numbers)
  Boolean d4 = defined("blue")

  # basename
  String path1 = basename("/A/B/C.txt")
  String path2 = basename("nuts_and_bolts.txt")
  File documents = "/src/tools/docs.md"
  String path3 = basename(documents)

  String path4 = basename("/A/B/C.txt", ".txt")
  String path5 = basename("/A/B/C.txt", ".text")

  # glob
  Array[File] texts = glob("*.txt")

  # read/write object
  Object o = object { author : "Benjamin", year : 1973, title : "Color the sky green" }
  File of = write_object(o)
  Object o2 = read_object(of)

  # read/write objects
  Object o3 = object { author : "Primo Levy", year : 1975, title : "The Periodic Table" }
  File of2 = write_objects([o, o3])
  Array[Object] arObj = read_objects(of2)

  # read/write json
  House house = {
    "city" : "Seattle",
    "team" : "Trail Blazers",
    "zipcode" : 98109
  }
  File hf = write_json(house)
  Object houseObj = read_json(hf)
}
