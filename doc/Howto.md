# Writing good WDL code

This page contains examples for writing good WDL code. Sometimes, the correct WDL way of
doing things is not immediately obvious, and here we provide some worked examples.

## Optional argument with a default

Examine a situation where you want to write a task that takes an
optional argument with a default. For example, the task `hello` takes
an optional username argument. If this is not supplied, the default is `dave.jones`.

```wdl
workflow foo {
    String? username
    hello { input: who = username }
}

task hello {
    String? who
    String who_actual = select_first([who, "dave.jones"])

    command {
        echo "Hello, ${who_actual}"
    }
    output {
        String result = read_string(stdout())
    }
}
```
