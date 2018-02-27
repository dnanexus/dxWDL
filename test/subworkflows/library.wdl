workflow review {
    String film

    call audience {
        input: film = film
    }
    call rotten_tomatoes as rt___X {
        input: film = film
    }

    Float avg_score = (audience.score + rt___X.score)/2
    output {
        Float score = avg_score
    }
}

task audience {
    String film
    command {}
    output {
        Float score = 0.8
    }
}

task rotten_tomatoes {
    String film
    command {}
    output {
        Float score = 0.4
    }
}
