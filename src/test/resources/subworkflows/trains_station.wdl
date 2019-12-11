version 1.0

import "trains.wdl" as trains

workflow ensure_trains {

    input {
            Array[String] prefixes
            Array[String] ranges
        }

    call trains.trains {
        input:
            prefixes=prefixes,
            ranges=ranges
    }

    output {
        Array[File] trains_out = trains.results
    }
}
