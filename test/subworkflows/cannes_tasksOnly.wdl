import "library.wdl" as lib

workflow cannes {
    Array[String] titles

    scatter (film in titles) {
        call lib.audience as audience {
            input: film= film
        }
    }

    output {
        audience.score
    }
}
