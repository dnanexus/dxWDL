
version development

import "library.wdl" as lib

workflow movies {
    input {
        String m_name
    }

    call lib.review as review {
        input: film = m_name
    }

    output {
        Float score = review.score
    }
}
