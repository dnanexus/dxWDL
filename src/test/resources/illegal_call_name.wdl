task review {
    String film
    command {}
    output {
        Float score = 4.3
    }
}

workflow illegal_call_name {
    Array[String] titles

    scatter (film in titles) {
        call review as review___two {
            input: film=film
        }
    }
    output {
        review___two.score
    }
}
