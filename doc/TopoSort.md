# Topological Sort of WDL Workflow in dxWDL

When compiling a WDL workflow to a DNAnexus workflow, an initial pass is performed to ensure that the workflow does not contain any directed cycles and that calls to tasks are sorted in order of their dependencies.  This is not a completely trivial task and there are some more complex topics to discuss.  We will begin with the basics and then go onto more advanced aspects of the sorting.

## Basics

In general, a workflow can be thought of as a directed graph, and most workflows that may not be dependent on inputs or more complex conditionals will typically not contain cycles.  In this sense the workflow is a directed acyclic graph (DAG) and the node sets of these types of graphs can be efficiently sorted in order of dependencies.

In the case of a simple WDL file, this is fairly straightforward.  For example:

```scala
task add {
    Int x
    Int y
    output { r = x + y }
}

workflow w {
    call add as C { input: x = A.r, y = 0 }
    call add as B { input: x = 3, y = 0 }
    call add as D { input: x = B.r, y = C.r }
    call add as A { input: x = 0, y = 0 }
}
```

Here, both A and B depend on no calls, C depends on the result of A, and D depends on the result of B and C.   Graphically this looks like:

```

A -> C ---> *-----*
            |  D  |
B --------> *-----*
```

The topological sorting will simply create an ordering like the one graphically depicted above where the nodes of the graph are sorted in a way that guarantees that no job v that w depends on will appear after w in the sorted list.

## More Complex Issues
WDL supports more complex workflows.  Specifically the concept of a `scatter` makes the process of sorting, and even defining the DAG bit more difficult.  Here is an example:

```scala
task concat {
    String a
    String b
    command {
       echo ${a} ${b} > foo.txt
    }
    output {
       String result = read_string("foo.txt")
         # Or read from stdout
    }
    runtime {
       # Docker image info etc ...
    }
}

task gather {
    Array[Array[String]] strings
    # ...
}

workflow {
    String a
    String b
    Array[String] xs = ["1","2","3"]
    call concat as cat { input: a = a, b = b }
    scatter (x in xs) {
        call concat as scat { input a = x, b = b }
        call concat as tcat { input a = scat.result, b = b }
        scatter (x in xs) {
            call concat as ncat { input a = tcat.result, b = b }
        }
    }
    call gather { strings = tcat.result }
}
```

The body of a `scatter` can contain what appears to be an arbitrary ‘sub workflow’.  However, this ‘sub workflow’ can be dependent on tasks anywhere outside the scatter context.  This makes the sorting problem a bit more difficult, and in particular can result in some hard-to-read workflows.

## Default sorting procedure

By default, we conceptually build a DAG at every level of the AST hierarchy and perform a topological sort the nodes within that level.   This implies that we **require** that at every level of the AST the graph is a DAG and there is no cycle.

For example, any dependencies between descendants of two scatters will be dependencies of the parent scatters.  If this causes a cycle, compilation of the dxWDL workflow will error out:

```scala
task add {
    Int x
    Int y
    output { r = x + y }
}

workflow W {
    Array[Int] xs
    scatter (x in xs) {
        call add as A { input: x = C.r, y = 0 }
        call add as B { input: x = 3, y = 0 }
    }
    scatter (x in xs) {
        call add as C { input: x = 1, y = 2 }
        call add as D { input: x = B.r, y = 0 }
    }
}
```

Here,  `A` depends on the result of `C` and `D` depends on the result of `B`.    This is not strictly a cycle, however in some senses, each scatter operation at the top level is dependent on one another.    More complex examples of this kind could cause difficulty in understanding the execution of a workflow.

The structure of this 'scatter collapsing' recursion looks like this at a high level (pseudocode):

```
# Returns a list of sorted nodes
tsort(edges)

# Returns a sorted workflow
sortWorkflow(workflow) {
    # Top level children of a workflow
    tsortASTNodes(wf.children)
}

# Sorts nodes at a particular level in the AST
tsortASTNodes(nodes) {
    recursedNodes = nodes.map [
        if node is scatter:
              newScatter(tsortASTNodes(scatter.children))
          else:
              node
    ]

    # Build dependency graph at this level
    edges  = # ...
    tsort(edges)
}
```

Building the graph (list of edges) itself is somewhat complex because of the requirement where we conceptually ‘collapse’ a scatter.  Building this  ‘scatter collapsed’ graph requires resolving two important cases:

Case 1:

Let `descendants(scatter)` be the set of all AST nodes recursively within a particular scatter and let `dependants(descendants(scatter))` be the set nodes dependent on those descendants.   Any dependent node outside the  scatter context (`dependants(descendants(scatter))  - descendants(scatter) - {scatter}`) should become a dependent of the scatter context itself.

Case 2:

If any node `v` in the current level of the AST has a parent within a scatter context at this level, make `v`’s parent the scatter itself.

Regarding implementation of this type of algorithm, one approach is to define a separate graph with node types for a new 'scatter collapsed' graph and recursively build this graph.  This new graph type will eventually have to be converted back into nodes in the original WDL AST.   The other approach is rely solely on nodes in the WDL AST to build the graph (in other words, /any/ graph topologically sorted in the AST will consist of nodes in the AST itself.  We opted for the latter approach though both have their advantages and disadvantages.

## Alternative 'relaxed' sorting procedure

Conceptually there are two major ways sorting a WDL workflow can be implemented. The procedure above is fairly strict in the sense that dependencies within scatters are 'collapsed' and propoagated up to scatters at each level.  It may be the case that, in some cases, it is more succinct and desirable to allow more complex dependencies between scatters as displayed in the example earlier. In this case, one can first topologically sort the dependency graph overlayed over the AST (i.e. all nodes at any depth in the AST). This sorted list of AST graph nodes can be used as an input to a recursion filtering for only the nodes at a particular level in the AST. This does not collapse scatters and thus allows the the types of dependencies between scatters, for example, that may make a workflow difficult to read.  Here is an example:

```scala

task T {
	Int x
	Int y
	output { res = x + y }
}

workflow W {
    Array[Int] xs

    scatter (x in xs) {
	      call T as b { input: x = x, y = a.res }
    }

    call T as a { input: x = 1, y = 2 }
}
```

Here, `b` depends on `a.res`  However the scatter only depends on `xs`.  Therefore without the scatter collapsing there is no way at the top level that the sorting procedure knows to place `a` before the scatter.


Psuedocode for the procedure:

```
# Returns a list of sorted nodes
tsort(edges)

# Returns a sorted workflow
sortWorkflowAlternative(workflow) {
    edges = # directed graph built from ALL nodes in the AST
    allNodesSorted = tsort(edges)
    # Top level children of a workflow
    tsortASTNodesAlternative(allNodesSorted, wf.children)
}

# Sorts nodes at a particular level in the AST
tsortASTNodesAlternative(allNodesSorted, nodes) {
    recursedNodes = nodes.map [
        if node is scatter:
              newScatter(tsortASTNodesAlternative(allNodesSorted, scatter.children))
          else:
              node
    ]

    filteredNodes = # Filter all sorted nodes by only those in the recursed nodes
}
```

## Are cycles OK?

Either sorting procedure above disallows cycles of dependencies between nodes in the AST.   In general this is because the workflow may not only be harder to read, but also introduce the possibility of deadlocks or an infinite loop:  e.g. the output of task A is dependent on the output of task B, but the output of task B is also dependent on the output of task A.   However, it could be the case, for example, that task A and B both participate in an iterative optimization that eventually converges.   TODO: am not clear yet to what extent these kinds of examples are desirable and supportable in WDL (e.g. an expectation-maximization algorithm represented in WDL).
