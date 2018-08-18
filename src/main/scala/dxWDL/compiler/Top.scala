// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import com.dnanexus.{DXDataObject, DXProject, DXSearch}
import dxWDL.{CompilerOptions, CompilationResults, DxPath, InstanceTypeDB, Utils, Verbose}
import dxWDL.Utils.DX_WDL_ASSET
import java.nio.file.{Files, Path, Paths}
import java.io.{FileWriter, PrintWriter}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.{Failure, Success}
import wdl.draft2.model.{WdlExpression, WdlNamespace, Draft2ImportResolver}

object Top {

    private def compileIR(bundle : wom.executable.WomBundle,
                          cOpt: CompilerOptions) : IR.Namespace = {
        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        val irNs = GenerateIR.apply(nsTree, cOpt.reorg, cOpt.locked, cOpt.verbose)
        val irNs2: IR.Namespace = cOpt.defaults match {
            case None => irNs
            case Some(path) => InputFile(cOpt.verbose).embedDefaults(irNs, path)
        }

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, None, irNs, cOpt.verbose.on)

        // generate dx inputs from the Cromwell-style input specification.
        cOpt.inputs.foreach{ path =>
            val dxInputs = InputFile(cOpt.verbose).dxFromCromwell(irNs2, path)
            // write back out as xxxx.dx.json
            val filename = Utils.replaceFileSuffix(path, ".dx.json")
            val parent = path.getParent
            val dxInputFile =
                if (parent != null) parent.resolve(filename)
                else Paths.get(filename)
            Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
            Utils.trace(cOpt.verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
        }
        irNs2
    }


    // Compile IR only
    def applyOnlyIR(wdlSourceFile: String,
                    cOpt: CompilerOptions) : IR.Namespace = {
        val bundle = Wom.getBundle(sourceFile)
        compileIR(bundle, cOpt)
    }
}
