
version 1.0

import "trains.wdl"


workflow trains_station {
    input {}

    call trains.trains {
        input:
            prefixes=["a", "b"],
            ranges=["1", "2"]
    }

    output {
        Array[String] out = trains.results
    }
}

