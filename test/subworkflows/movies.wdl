import "library.wdl" as lib

workflow movies {
    Array[String] m_names

    scatter (film in m_names) {
        call lib.review as review {
            input: film=film
        }
    }

    output {
        review.score
    }
}
