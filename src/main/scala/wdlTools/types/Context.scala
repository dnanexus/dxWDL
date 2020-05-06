package wdlTools.types

import java.net.URL
import wdlTools.syntax.AbstractSyntax._
import wdlTools.syntax.TextSource
import wdlTools.types.WdlTypes._

// An entire context
//
// There are separate namespaces for variables, struct definitions, and callables (tasks/workflows).
// An additional variable holds a list of all imported namespaces.
case class Context(docSourceUrl: Option[URL] = None,
                   declarations: Map[String, WT] = Map.empty,
                   structs: Map[String, WT_Struct] = Map.empty,
                   callables: Map[String, WT_Callable] = Map.empty,
                   namespaces: Set[String] = Set.empty) {

  def bindVar(varName: String, wdlType: WT, srcText: TextSource): Context = {
    declarations.get(varName) match {
      case None =>
        this.copy(declarations = declarations + (varName -> wdlType))
      case Some(_) =>
        throw new TypeException(s"variable ${varName} shadows an existing variable",
                                srcText,
                                docSourceUrl)
    }
  }

  def bind(s: WT_Struct, srcText: TextSource): Context = {
    structs.get(s.name) match {
      case None =>
        this.copy(structs = structs + (s.name -> s))
      case Some(_) =>
        throw new TypeException(s"struct ${s.name} is already declared", srcText, docSourceUrl)
    }
  }

  // add a callable (task/workflow)
  def bindCallable(callable: WT_Callable, srcText: TextSource): Context = {
    callables.get(callable.name) match {
      case None =>
        this.copy(callables = callables + (callable.name -> callable))
      case Some(_) =>
        throw new TypeException(s"a callable named ${callable.name} is already declared",
                                srcText,
                                docSourceUrl)
    }
  }

  // add a bunch of bindings
  def bindVarList(bindings: Map[String, WT], srcText: TextSource): Context = {
    val existingVarNames = declarations.keys.toSet
    val newVarNames = bindings.keys.toSet
    val both = existingVarNames intersect newVarNames
    if (both.nonEmpty)
      throw new TypeException(s"Variables ${both} are being redeclared", srcText, docSourceUrl)
    this.copy(declarations = declarations ++ bindings)
  }

  // When we import another document all of its definitions are prefixed with the
  // namespace name.
  //
  // -- library.wdl --
  // task add {}
  // workflow act {}
  //
  // import "library.wdl" as lib
  // workflow hello {
  //    call lib.add
  //    call lib.act
  // }
  def bindImportedDoc(namespace: String,
                      iCtx: Context,
                      aliases: Vector[ImportAlias],
                      srcText: TextSource): Context = {
    if (this.namespaces contains namespace)
      throw new TypeException(s"namespace ${namespace} already exists", srcText, iCtx.docSourceUrl)

    // There cannot be any collisions because this is a new namespace
    val iCallables = iCtx.callables.map {
      case (name, taskSig: WT_Task) =>
        val fqn = namespace + "." + name
        fqn -> taskSig.copy(name = fqn)
      case (name, wfSig: WT_Workflow) =>
        val fqn = namespace + "." + name
        fqn -> wfSig.copy(name = fqn)
      case other =>
        throw new Exception(s"sanity: ${other.getClass}")
    }

    // rename the imported structs according to the aliases
    //
    // import http://example.com/another_exampl.wdl as ex2
    //     alias Parent as Parent2
    //     alias Child as Child2
    //     alias GrandChild as GrandChild2
    //
    val aliasesMap: Map[String, String] = aliases.map {
      case ImportAlias(src, dest, _) => src -> dest
    }.toMap
    val iStructs = iCtx.structs.map {
      case (name, iStruct) =>
        aliasesMap.get(name) match {
          case None          => name -> iStruct
          case Some(altName) => altName -> WT_Struct(altName, iStruct.members)
        }
    }

    // check that the imported structs do not step over existing definitions
    val doublyDefinedStructs = this.structs.keys.toSet intersect iStructs.keys.toSet
    for (sname <- doublyDefinedStructs) {
      if (this.structs(sname) != iStructs(sname))
        throw new TypeException(s"Struct ${sname} is already defined in a different way",
                                srcText,
                                iCtx.docSourceUrl)
    }

    this.copy(structs = structs ++ iStructs,
              callables = callables ++ iCallables,
              namespaces = namespaces + namespace)
  }
}
