package dxWDL.Base

import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}
import wdlTools.eval.WdlValues

object WdlCompat {
  // types
  type WdlBooleanType = WdlTypes.T_Boolean
  type WdlIntegerType = WdlTypes.T_Int
  type WdlFloatType = WdlTypes.T_Float
  type WdlStringType = WdlTypes.T_String
  type WdlFileType = WdlTypes.T_File
  type WdlSingleFileType = WdlTypes.T_File
  type WdlArrayType = WdlTypes.T_Array

  type WdlMapType = WdlTypes.T_Map
  type WdlArrayType = WdlTypes.T_Array
  type WdlOptionalType = WdlTypes.T_Optional
  type WdlPairType = WdlTypes.T_Pair
  type WdlStructType = WdlTypes.T_Struct

  // values
  type WdlBoolean = WdlValues.V_Boolean
  type WdlInteger = WdlValues.V_Int
  type WdlFloat = WdlValues.V_Float
  type WdlString = WdlValues.V_String
  type WdlFile = WdlValues.V_File

  type WdlArray = WdlValues.V_Array
  type WdlMap = WdlValues.V_Map
  type WdlOptional = WdlValues.V_Optional
  type WdlOptionalValue = WdlValues.V_Optional
  type WdlPair = WdlValues.V_Pair
  type WdlStruct = WdlValues.V_Struct

  type WdlExpression = TAT.Expr
}
