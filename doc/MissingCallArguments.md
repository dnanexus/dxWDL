# Missing arguments

A workflow with missing call arguments is legal in WDL. For example,
workflow `trivial` calls task `add` with parameter `a` where
parameters `a` and `b` are required. If the user supplies
`b` at runtime, the workflow will execute correctly, otherwise, a
*missing argument* error will result.

```
workflow trivial {
    input {
      Int x
    }
    call add {
      input: a=x
    }
    output {
        Int sum = add.result
    }
}

task add {
  input {
    Int a
    Int b
  }
  command {}
  output {
    Int result = a + b
  }
}
```

The JSON inputs file below specifies the value `b`:
```
{
  "trivial.x" : 1,
  "trivial.add.b" : 3
}
```

In effect, missing call arguments are inputs to the workflow. This
also works for optional arguments. For example, if `add` has optional
argument `c`, then, it is possible to set it from a file:
```
{
  "trivial.x" : 1,
  "trivial.add.b" : 3,
  "trivial.add.c" : 10
}
```

This means that workflow `trivial` actually has four inputs: `{x, add.a, add.b, add.c}`.
