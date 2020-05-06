version 1.0

# create some files
task create {
  command {}
  output {
    Array[File] intervals = ["a", "b"]
  }
}

workflow foo {
  input {
    File? model
  }

  Int a = length([1, 2, 3])
  Float x = 3

  if (false) {
    call create
    Array[File] f1 = create.intervals
  }

  if (true) {
    scatter (i in [1, 2, 3]) {
      call create as c2
      Array[File] f2 = c2.intervals
    }

    Array[Array[File]] f3 = f2
  }

  Array[Array[File]]? f4 = f2


  File f = "foo"
  File f5 = f + "/all"

  # check that we can convert T to T? in this way
  String? s = "nothing much"
}
