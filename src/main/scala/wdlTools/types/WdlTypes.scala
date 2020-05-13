package wdlTools.types

// This is the WDL typesystem
object WdlTypes {
  sealed trait T

  // primitive types
  case object T_Boolean extends T
  case object T_Int extends T
  case object T_Float extends T
  case object T_String extends T
  case object T_File extends T
  case object T_Directory extends T

  // There are cases where we don't know the type. For example, an empty array, or an empty map.
  // While evaluating the right hand side we don't know the type.
  //
  // Array[Int] names = []
  // Map[String, File] locations = {}
  //
  case object T_Any extends T

  // Polymorphic functions are another place where type variables appear
  case class T_Var(i: Int) extends T

  // a user defined struct name
  case class T_Identifier(id: String) extends T

  // compound types
  case class T_Pair(l: T, r: T) extends T
  case class T_Array(t: T, nonEmpty: Boolean = false) extends T
  case class T_Map(k: T, v: T) extends T
  case object T_Object extends T
  case class T_Optional(t: T) extends T

  // a user defined structure
  case class T_Struct(name: String, members: Map[String, T]) extends T

  // Anything that can be called. A general group that includesw tasks
  // and workflows
  //
  sealed trait T_Callable extends T {
    val name: String
    val input: Map[String, (T, Boolean)]
    val output: Map[String, T]
  }

  // The type of a task.
  //
  // It takes typed-inputs and returns typed-outputs. The boolean flag denotes
  // if input is optional
  case class T_Task(name: String, input: Map[String, (T, Boolean)], output: Map[String, T])
      extends T_Callable

  // The type of a workflow.
  // It takes typed-inputs and returns typed-outputs.
  case class T_Workflow(name: String, input: Map[String, (T, Boolean)], output: Map[String, T])
      extends T_Callable

  // Result from calling a task or a workflow.
  case class T_Call(name: String, output: Map[String, T]) extends T

  // A standard library function implemented by the engine.
  sealed trait T_StdlibFunc extends T {
    val name: String
    val output: T
  }

  // T representation for an stdlib function.
  // For example, stdout()
  case class T_Function0(name: String, output: T) extends T_StdlibFunc

  // A function with one argument
  // Example:
  //   read_int(FILE_NAME)
  //   Array[Array[X]] transpose(Array[Array[X]])
  case class T_Function1(name: String, input: T, output: T) extends T_StdlibFunc

  // A function with two arguments. For example:
  //   Float size(File, [String])
  //   Array[Pair(X,Y)] zip(Array[X], Array[Y])
  case class T_Function2(name: String, arg1: T, arg2: T, output: T) extends T_StdlibFunc

  // A function with three arguments. For example:
  // String sub(String, String, String)
  case class T_Function3(name: String, arg1: T, arg2: T, arg3: T, output: T) extends T_StdlibFunc
}
