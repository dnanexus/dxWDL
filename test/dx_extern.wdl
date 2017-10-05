task native_concat {
    String? a 
    String? b 
    command {
    }
    output {
        String c = ""
    }
    meta {
        type: "native" 
        id: "applet-F7F8xgQ0ZvgQXBVjK6BPQjbb" 
    }
}
task native_diff {
    File a 
    File b 
    command {
    }
    output {
        Boolean equality = true
    }
    meta {
        type: "native" 
        id: "applet-F7F8xj00ZvgbY8V4K5vj3V8p" 
    }
}
task native_mk_list {
    Int a 
    Int b 
    command {
    }
    output {
        Array[Int] all = []
    }
    meta {
        type: "native" 
        id: "applet-F7F8xj80ZvgQxk26K6ZpjVBQ" 
    }
}
task native_sum {
    Int? a 
    Int? b 
    command {
    }
    output {
        Int result = 0
    }
    meta {
        type: "native" 
        id: "applet-F7F8xjQ0ZvgbY8V4K5vj3V8v" 
    }
}