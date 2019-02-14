# Tips and tricks

This page contains examples of coding patterns in WDL. Sometimes, the correct WDL way of
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

## Assertions

Currently, WDL does not have a way to throw exceptions, or report errors. A way around
this limitation is to write an `assert` task (suggested by [@mlin](https://github.com/mlin)).

```wdl
task assert {
    Boolean value
    String msg

    command {
        if [ "${value}" == "false" ]; then
            echo $msg
            exit 1
        fi
        exit 0
    }
}
```

You can call it from a workflow when you want to check a condition.

```wdl
workflow foo {
    call assert { input: value= CONDITION_TO_CHECK,
                         msg = ERROR_MESSAGE }
}
```


A method that works directly with the platform error reporting mechanism, suggested by [@jtratner](https://github.com/jtratner), is:

```wdl
workflow show_error {
    call make_error {}
}

task make_error {
    command <<<
        echo '{"error": {"type": "AppError", "message": "x must be at least 2"}}' > job_error.json
        exit 1
    >>>
}
```
