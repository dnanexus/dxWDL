package dxWDL.compiler

// A bunch of WDL source lines
case class WdlCodeSnippet(value : String)

case class WdlCodeGen(verbose: Verbose) {

    def genDefaultValueOfType(wdlType: WomType) : WomValue = {
        wdlType match {
            case WomBooleanType => WomBoolean(true)
            case WomIntegerType => WomInteger(0)
            case WomFloatType => WomFloat(0.0)
            case WomStringType => WomString("")
            case WomSingleFileType => WomSingleFile("")

                // We could convert an optional to a null value, but that causes
                // problems for the pretty printer.
                // WomOptionalValue(wdlType, None)
            case WomOptionalType(t) => genDefaultValueOfType(t)

            case WomObjectType =>
                // This fails when trying to do a 'toWomString'
                // operation.
                //WomObject(Map("_" -> WomString("_")), WomObjectType)
                WomObject(Map.empty)


                // The WomMap type HAS to appear before the array types, because
                // otherwise it is coerced into an array. The map has to
                // contain at least one key-value pair, otherwise you get a type error.
            case WomMapType(keyType, valueType) =>
                val k = genDefaultValueOfType(keyType)
                val v = genDefaultValueOfType(valueType)
                WomMap(WomMapType(keyType, valueType), Map(k -> v))

                // an empty array
            case WomMaybeEmptyArrayType(t) =>
                WomArray(WomMaybeEmptyArrayType(t), List())

                // Non empty array
            case WomNonEmptyArrayType(t) =>
                WomArray(WomNonEmptyArrayType(t), List(genDefaultValueOfType(t)))

            case WomPairType(lType, rType) => WomPair(genDefaultValueOfType(lType),
                                                      genDefaultValueOfType(rType))
            case _ => throw new Exception(s"Unhandled type ${wdlType.toDisplayString}")
        }
    }

/*
Create a stub for an applet. This is an empty task
that includes the input and output definitions. It is used
to allow linking to native DNAx applets (and workflows in the future).

For example, the stub for the Add task:
task Add {
    input {
      Int a
      Int b
    }
    command {
    command <<<
        python -c "print(${a} + ${b})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

is:
task Add {
   input {
     Int a
     Int b
   }
   command {}
   output {
       Int result
   }
  }
*/
    def appletStub(callable: IR.Callable) : WdlCodeSnippet = {
        Utils.trace(verbose2,
                    s"""|genAppletStub  callable=${callable.name}
                        |  inputs= ${callable.inputVars.map(_.name)}
                        |  outputs= ${callable.outputVars.map(_.name)}"""
                        .stripMargin)

        val inputs = callable.inputVars.map{ cVar =>
            s"${cVar.womType.toDisplayString} ${cVar.name}"
        }.mkString("\n")

        val outputs = callable.outputVars.map{ cVar =>
            val defaultVal = genDefaultValueOfType(cVar.womType)
            s"${cVar.womType.toDisplayString} ${cVar.name} = ${defaultVal}"
        }.mkString("\n")

        // We are using WDL version 1.0 here. The input syntax is not
        // available prior.
        s"""|task ${callable.name} {
            |  input {
            |    ${inputs}
            |  }
            |  command {}
            |  output {
            |    ${outputs}
            |  }
            |}""".stripMargin

        task.children = inputs ++ outputs
        task
    }
}
