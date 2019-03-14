# A sub-block that is compiled into a subworkflow
import "check_route.wdl" as lib

workflow trains {
    Array[String] prefixes
    Array[String] ranges

    scatter (p in prefixes) {
        call lib.check_route as check_route {
            input: prefix=p,
                   ranges=ranges
        }
    }

    Array[String] f_results = flatten(check_route.result)
    output {
        Array[String] results = f_results
    }
}
