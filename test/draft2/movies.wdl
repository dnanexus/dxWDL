import "library.wdl" as lib

workflow movies {
    String m_name
    call lib.review as review {
        input: film = m_name
    }
    output {
        Float score = review.score
    }
}
