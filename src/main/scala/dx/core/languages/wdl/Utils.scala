package dx.core.languages.wdl

import java.nio.file.Path

import dx.core.languages.Language
import dx.core.languages.Language.Language
import wdlTools.syntax.{Parsers, SourceLocation, SyntaxException, WdlParser, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.{
  TypeCheckingRegime,
  TypeException,
  TypeInfer,
  WdlTypes,
  TypedAbstractSyntax => TAT
}
import wdlTools.util.{FileSource, FileSourceResolver, Logger}

object Utils {
  val locPlaceholder: SourceLocation = SourceLocation.empty

  // A self contained WDL workflow
  def getWdlVersion(language: Language): WdlVersion = {
    language match {
      case Language.WDLvDraft2 => WdlVersion.Draft_2
      case Language.WDLv1_0    => WdlVersion.V1
      case Language.WDLv2_0    => WdlVersion.V2
      case other =>
        throw new Exception(s"Unsupported language version ${other}")
    }
  }

  // create a wdl-value of a specific type.
  def genDefaultValueOfType(wdlType: WdlTypes.T,
                            loc: SourceLocation = Utils.locPlaceholder): TAT.Expr = {
    wdlType match {
      case WdlTypes.T_Boolean => TAT.ValueBoolean(value = true, wdlType, loc)
      case WdlTypes.T_Int     => TAT.ValueInt(0, wdlType, loc)
      case WdlTypes.T_Float   => TAT.ValueFloat(0.0, wdlType, loc)
      case WdlTypes.T_String  => TAT.ValueString("", wdlType, loc)
      case WdlTypes.T_File    => TAT.ValueString("placeholder.txt", wdlType, loc)

      // We could convert an optional to a null value, but that causes
      // problems for the pretty printer.
      // WdlValues.V_OptionalValue(wdlType, None)
      case WdlTypes.T_Optional(t) => genDefaultValueOfType(t)

      // The WdlValues.V_Map type HAS to appear before the array types, because
      // otherwise it is coerced into an array. The map has to
      // contain at least one key-value pair, otherwise you get a type error.
      case WdlTypes.T_Map(keyType, valueType) =>
        val k = genDefaultValueOfType(keyType)
        val v = genDefaultValueOfType(valueType)
        TAT.ExprMap(Map(k -> v), wdlType, loc)

      // an empty array
      case WdlTypes.T_Array(_, false) =>
        TAT.ExprArray(Vector.empty, wdlType, loc)

      // Non empty array
      case WdlTypes.T_Array(t, true) =>
        TAT.ExprArray(Vector(genDefaultValueOfType(t)), wdlType, loc)

      case WdlTypes.T_Pair(lType, rType) =>
        TAT.ExprPair(genDefaultValueOfType(lType), genDefaultValueOfType(rType), wdlType, loc)

      case WdlTypes.T_Struct(_, typeMap) =>
        val members = typeMap.map {
          case (fieldName, t) =>
            val key: TAT.Expr = TAT.ValueString(fieldName, WdlTypes.T_String, loc)
            key -> genDefaultValueOfType(t)
        }
        TAT.ExprObject(members, wdlType, loc)

      case _ => throw new Exception(s"Unhandled type ${wdlType}")
    }
  }

  /**
    * Parses a top-level WDL file and all its imports.
    * @param path the path to the WDL file
    * @param fileResolver FileSourceResolver
    * @param regime TypeCheckingRegime
    * @param logger Logger
    * @return (document, aliases), where aliases is a mapping of all the (fully-qualified)
    *         alias names to values. Aliases include Structs defined in any file (which are
    *         *not* namespaced) and all aliases defined in import statements of all documents
    *         (which *are* namespaced).
    */
  def parseSource(path: Path,
                  fileResolver: FileSourceResolver = FileSourceResolver.get,
                  regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                  logger: Logger = Logger.get): (TAT.Document, Map[String, WdlTypes.T_Struct]) = {
    val sourceCode = fileResolver.fromPath(path)
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    parseSource(parser, sourceCode, fileResolver, regime, logger)
  }

  def parseSource(parser: WdlParser,
                  sourceCode: FileSource,
                  fileResolver: FileSourceResolver = FileSourceResolver.get,
                  regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                  logger: Logger = Logger.get): (TAT.Document, Map[String, WdlTypes.T_Struct]) = {
    try {
      val doc = parser.parseDocument(sourceCode)
      val (tDoc, ctx) =
        TypeInfer(regime, fileResolver = fileResolver, logger = logger)
          .apply(doc)
      (tDoc, ctx.aliases)
    } catch {
      case se: SyntaxException =>
        logger.error(s"WDL code is syntactically invalid -----\n${sourceCode}")
        throw se
      case te: TypeException =>
        logger.error(
            s"WDL code is syntactically valid BUT it fails type-checking -----\n${sourceCode}"
        )
        throw te
    }
  }
}
