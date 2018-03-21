import "library.wdl" as lib

workflow cannes {
    Array[String] titles

    scatter (film in titles) {
        call lib.review as review {
            input: film=film
        }
    }
    output {
        Array[Float] score = review.score
    }
}
