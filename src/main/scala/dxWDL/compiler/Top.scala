// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import dxWDL.{CompilerOptions, InstanceTypeDB, Utils, Verbose}
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

object Top {

    // Compile IR only
    def applyOnlyIR(sourceFile: String,
                    cOpt: CompilerOptions) : IR.Bundle = {
        val bundle : wom.executable.WomBundle = Wom.getBundle(sourceFile)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        GenerateIR.applyOnlyIR(bundle, cOpt.verbose)
    }
}
