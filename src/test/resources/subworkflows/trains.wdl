version 1.0

# A sub-block that is compiled into a subworkflow
import "check_route.wdl" as lib

workflow trains {
    input {
        Array[String] prefixes
        Array[String] ranges
    }

    scatter (p in prefixes) {
        call lib.check_route as check_route {
            input: prefix=p, ranges=ranges
        }
    }

    output {
        Array[String] results = flatten(check_route.result)
    }
}
