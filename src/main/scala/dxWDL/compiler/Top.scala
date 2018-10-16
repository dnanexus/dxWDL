// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler


import dxWDL.{CompilerOptions, ParseWomSourceFile}

object Top {

    // Compile IR only
    def applyOnlyIR(sourceFile: String,
                    cOpt: CompilerOptions) : IR.Bundle = {
        val bundle = ParseWomSourceFile.apply(sourceFile)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        GenerateIR.apply(bundle, cOpt.verbose)
    }
}
