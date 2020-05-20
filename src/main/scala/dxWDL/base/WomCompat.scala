package dxWDL.base

import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}
import wdlTools.eval.WdlValues

object WomCompat {
  // types
  type WomType = WdlTypes.T
  type WomBooleanType = WdlTypes.T_Boolean
  type WomIntegerType = WdlTypes.T_Int
  type WomFloatType = WdlTypes.T_Float
  type WomStringType = WdlTypes.T_String
  type WomFileType = WdlTypes.T_File
  type WomSingleFileType = WdlTypes.T_File

  type WomMapType = WdlTypes.T_Map
  type WomArrayType = WdlTypes.T_Array
  type WomOptionalType = WdlTypes.T_Optional
  type WomPairType = WdlTypes.T_Pair
  type WomStructType = WdlTypes.T_Struct

  // values
  type WomValue = WdlValues.V
  type WomBoolean = WdlValues.V_Boolean
  type WomInteger = WdlValues.V_Int
  type WomFloat = WdlValues.V_Float
  type WomString = WdlValues.V_String
  type WomFile = WdlValues.V_File

  type WomArray = WdlValues.V_Array
  type WomMap = WdlValues.V_Map
  type WomOptional = WdlValues.V_Optional
  type WomOptionalValue = WdlValues.V_Optional
  type WomPair = WdlValues.V_Pair
  type WomStruct = WdlValues.V_Struct

  type WomExpression = TAT.Expr
  type WorkflowSource = String
}
