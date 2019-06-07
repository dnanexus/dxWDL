version 1.0

struct Foo {
  String bar
}

struct Bunk {
  File blork
}

struct SampleReports {
    String sample_name
    Array[File] reports
}

struct SampleReportsArray {
    Array[SampleReports]+ sample_reports
}

struct Coord {
    Int x
    Int y
}
