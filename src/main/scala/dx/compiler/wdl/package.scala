package dx.compiler.wdl

import dx.compiler.ir._
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}
import wdlTools.util.Adjuncts

case class WdlBundle(version: WdlVersion,
                     primaryCallable: Option[TAT.Callable],
                     tasks: Map[String, TAT.Task],
                     workflows: Map[String, TAT.Workflow],
                     callableNames: Set[String],
                     sources: Map[String, TAT.Document],
                     adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

object Utils {
  def wdlToIRType(wdlType: WdlTypes.T): Type = {
    wdlType match {
      case WdlTypes.T_Boolean     => TBoolean
      case WdlTypes.T_Int         => TInt
      case WdlTypes.T_Float       => TFloat
      case WdlTypes.T_String      => TString
      case WdlTypes.T_File        => TFile
      case WdlTypes.T_Directory   => TDirectory
      case WdlTypes.T_Array(t, _) => TArray(wdlToIRType(t))
      case WdlTypes.T_Object      => THash
      case _ =>
        throw new Exception(s"Cannot convert WDL type ${wdlType} to IR")
    }
  }
}
