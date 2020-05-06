version 1.0

task wc {
  input {
    File f
    Boolean l = false
    String? region
  }

  command {
    wc ${true='-l' false=' ' l} ${f}
  }

  parameter_meta {
    f : { help: "Count the number of lines in this file" }
    l : { help: "Count only lines" }
    region: {help: "Cloud region",
             suggestions: ["us-west", "us-east", "asia-pacific", "europe-central"]}
  }

  output {
     String retval = stdout()
  }
}
