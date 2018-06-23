# Missing arguments

A workflow with missing call arguments is legal in WDL. For example,
workflow `trivial` calls task `add` with parameter `a` where
parameters `a` and `b` are required. If the user supplies
`b` at runtime, the workflow will execute correctly, otherwise, a
*missing argument* error will result.

```wdl
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

The JSON inputs file below specifies `b`:
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

This means that workflow `trivial` actually has four inputs: `{x,
add.a, add.b, add.c}`. A large realistic workflow makes many calls,
and has many hidden arguments. To implement this with a dnanexus
workflow, we need to materialize all these inputs, they cannot remain
hidden. The resulting platform workflow could easily have tens or
hundreds of inputs, making the user interface interface ungainly.

## Implementation issues

An implementation will need to start by adding the missing arguments. Let's
take a look at adding `trivial.add.b`. The workflow is rewritten to:

```wdl
workflow trivial {
    input {
      Int x
      Int add_b
    }
    call add {
      input: a=x, b=add_b
    }
    output {
        Int sum = add.result
    }
}
```

When reading the input file, we need to translate `trivial.add.b` to `trivial.add_b`. This requires
a mapping from workflow input arguments to their original names. This is extra
metadata for the compilation process; it is not simply just additional WDL code.

| new name | original |
| -------- | --------   |
| trivial.x | trivial.x  |
| trivial.add_b | trivial.add.b |

If a variable named `add_b` already exists, a new name is required for `add.b`.
Each workflow can go through multiple rewrite steps, each of which may encounter
naming collisions. For a complex workflow, the end result could be so different from
the original, as to be unrecognizable. Because names are mangled, following what
happens are runtime in the UI will be hard.
