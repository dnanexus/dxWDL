package wdlTools.util

import java.io.EOFException

import wdlTools.util.InteractiveConsole.{Reader, _}
import wdlTools.util.Util.BiMap

import scala.collection.mutable

case class InteractiveConsole(promptColor: String = "",
                              separator: String = ": ",
                              afterEntry: Option[String] = None) {

  def println(text: String, style: String = ""): Unit = {
    Console.println(s"${style}${text}${Console.RESET}")
  }

  def title(text: String): Unit = {
    println(text, s"${promptColor}${Console.UNDERLINED}")
  }

  def error(text: String): Unit = {
    println(text, Console.RED)
  }

  def resolveDefault[T](default: Option[T],
                        choices: Option[BiMap[String, T]] = None,
                        optional: Boolean = false): (Option[T], Option[String]) = {
    val resolvedDefault = default match {
      case v: Some[T] => v
      case None if choices.nonEmpty && choices.get.size == 1 && !optional =>
        Some(choices.get.values.head)
      case _ => None
    }
    val defaultKey = if (resolvedDefault.isDefined && choices.isDefined) {
      if (!choices.get.values.contains(resolvedDefault.get)) {
        throw new RuntimeException("choices list does not contain default value")
      }
      Some(choices.get.fromValue(resolvedDefault.get))
    } else {
      None
    }
    (resolvedDefault, defaultKey)
  }

  def getPrompt[T](prefix: String,
                   optional: Boolean = false,
                   default: Option[T] = None,
                   choices: Option[BiMap[String, T]] = None,
                   index: Int = -1,
                   maxIndex: Option[Int] = None): String = {
    val attrs = new StringBuilder()
    val max = if (maxIndex.isDefined) {
      maxIndex.get.toString
    } else {
      "N"
    }
    if (index > 0) {
      attrs.append(s" (${index}/${max})")
    }
    choices match {
      case Some(values) if default.nonEmpty =>
        val defaultKey = values.fromValue(default.get)
        val rest = values.filterKeys(_ != defaultKey)
        if (rest.size == values.size) {
          throw new RuntimeException("choices must contain default value")
        }
        attrs.append(s" [${Console.UNDERLINED}${defaultKey}${Console.RESET}${promptColor}")
        rest.keys.foreach(item => attrs.append(s", ${item}"))
        attrs.append("]")
      case Some(values)          => attrs.append(s" s[${values.keys.mkString(",")}]")
      case _ if default.nonEmpty => attrs.append(s" [${default.get}]")
      case _                     => ()
    }
    if (optional && !attrs.endsWith("]")) {
      attrs.append("*")
    }
    s"${promptColor}${prefix}${attrs}${Console.RESET}${separator}"
  }

  def promptLoop[T](prompt: String,
                    reader: Reader[T],
                    optional: Boolean,
                    default: Option[T] = None): Option[T] = {
    def promptOnce: Option[T] = {
      Console.print(prompt)
      reader.read match {
        case v: Some[T]               => v
        case None if default.nonEmpty => default
        case _                        => None
      }
    }
    def promptWhileError: Option[T] = {
      while (true) {
        try {
          return promptOnce
        } catch {
          case e: InputException =>
            error(s"You entered an invalid value: ${e.getMessage}; please try again")
        }
      }
      throw new Exception()
    }
    var result = promptWhileError
    while (result.isEmpty && !optional) {
      error("A non-empty value is required; please try again")
      result = promptWhileError
    }
    if (afterEntry.isDefined) {
      Console.print(afterEntry.get)
    }
    result
  }

  /**
    * As the user to enter a value.
    * @param promptPrefix the prompt prefix
    * @param optional whether the user's response is optional
    * @param default the default value to return when the user does not enter a value
    * @param choices a sequence of allowed choices
    * @param choicesMap allowed choices, defined as a mapping of display values to return values
    * @param reader StdIn reader
    * @tparam T the type to return
    * @return the value the user entered, converted to type T
    */
  def ask[T](promptPrefix: String,
             optional: Boolean = false,
             default: Option[T] = None,
             choices: Option[Seq[T]] = None,
             choicesMap: Option[BiMap[String, T]] = None,
             menu: Option[Boolean] = None,
             otherOk: Boolean = false,
             otherOption: String = "Other",
             multiple: Boolean = false)(
      implicit reader: InteractiveConsole.Reader[T]
  ): Seq[T] = {
    val finalChoices = choicesMap.orElse(choices.map(choicesToMap)).orElse(reader.defaultChoices)
    val (defaultValue, defaultKey) = resolveDefault(default, finalChoices, optional)

    val getOnce: (Boolean, Int) => Option[T] = if (menu.getOrElse(finalChoices.isDefined)) {
      val otherKey = if (otherOk) {
        Vector(otherOption)
      } else {
        Vector.empty[String]
      }
      val choiceKeys: Seq[String] = finalChoices.get.keys.toVector ++ otherKey
      val defaultChoice = if (finalChoices.isDefined && defaultKey.isDefined) {
        Some(finalChoices.get.keys.indexOf(defaultKey.get))
      } else {
        None
      }
      choiceKeys.zipWithIndex.foreach(choice => Console.println(s"${choice._2}) ${choice._1}"))
      def getOnce(optional: Boolean, n: Int): Option[T] = {
        val prompt: String = getPrompt(promptPrefix, optional, defaultValue, index = n)
        val choice =
          promptLoop[Int](prompt, new RangeReader(0, choiceKeys.size), optional, defaultChoice)
        if (choice.isDefined) {
          val key = choiceKeys(choice.get)
          if (otherOk && key == otherOption) {
            promptLoop[T](
                getPrompt(prefix = "Enter other value", optional = optional),
                reader,
                optional
            )
          } else {
            Some(finalChoices.get.fromKey(key))
          }
        } else {
          None
        }
      }
      getOnce
    } else {
      def getOnce(optional: Boolean, n: Int): Option[T] = {
        val prompt = getPrompt(promptPrefix, optional, defaultValue, finalChoices, n)
        promptLoop[T](prompt, reader, optional, defaultValue)
      }
      getOnce
    }

    var n = if (multiple) {
      1
    } else {
      -1
    }
    val firstResult = getOnce(optional, n)

    if (firstResult.isEmpty) {
      Vector.empty
    } else if (multiple) {
      val result: mutable.Buffer[T] = mutable.ArrayBuffer(firstResult.get)
      var continue = true
      while (continue) {
        n += 1
        val nextResult = getOnce(optional, n)
        if (nextResult.isDefined) {
          result.append(nextResult.get)
        } else {
          continue = false
        }
      }
      result.toVector
    } else {
      Vector(firstResult.get)
    }
  }

  def askOnce[T](
      prompt: String,
      optional: Boolean = false,
      default: Option[T] = None,
      choices: Option[Seq[T]] = None,
      choicesMap: Option[BiMap[String, T]] = None,
      menu: Option[Boolean] = None,
      otherOk: Boolean = false,
      otherOption: String = "Other"
  )(implicit reader: InteractiveConsole.Reader[T]): Option[T] = {
    ask[T](prompt,
           optional,
           default,
           choices,
           choicesMap,
           menu,
           otherOk,
           otherOption,
           multiple = false) match {
      case Seq(result) => Some(result)
      case _           => None
    }
  }

  def askRequired[T](
      prompt: String,
      default: Option[T] = None,
      choices: Option[Seq[T]] = None,
      choicesMap: Option[BiMap[String, T]] = None,
      menu: Option[Boolean] = None,
      otherOk: Boolean = false
  )(implicit reader: InteractiveConsole.Reader[T]): T = {
    ask[T](prompt, optional = true, default, choices, choicesMap, menu, otherOk, multiple = false) match {
      case Seq(result) => result
      case _           => throw new RuntimeException("Expected result for required value")
    }
  }

  def askYesNo(prompt: String, default: Option[Boolean] = None): Boolean = {
    askOnce[Boolean](prompt,
                     optional = false,
                     default = default,
                     choicesMap = Some(BiMap(Vector("yes", "no"), Vector(true, false))),
                     menu = Some(false)).get
  }
}

object InteractiveConsole {
  class InputException(cause: Throwable) extends Exception(cause)

  def choicesToMap[T](choices: Seq[T]): BiMap[String, T] = {
    BiMap.fromMap(choices.map(choice => choice.toString -> choice).toMap)
  }

  abstract class Reader[T] {
    def defaultChoices: Option[BiMap[String, T]] = None

    /**
      * Read a value from StdIn.
      * @return The value converted to the specified type, or None if no value was entered.
      * @throws InputException if the entered value was invalid
      */
    def read: Option[T]
  }

  class IntReader extends Reader[Int] {
    def read: Option[Int] = {
      try {
        Some(io.StdIn.readInt())
      } catch {
        case _: EOFException          => None
        case e: NumberFormatException => throw new InputException(e)
      }
    }
  }

  class RangeReader(min: Int, max: Int) extends IntReader {
    override def read: Option[Int] = {
      val value = super.read
      if (value.isDefined) {
        require(value.get >= min)
        require(value.get < max)
      }
      value
    }
  }

  object Reader {
    implicit val stringReader: Reader[String] = new Reader[String] {
      def read: Option[String] = {
        io.StdIn.readLine() match {
          case ""   => None
          case line => Some(line)
        }
      }
    }

    implicit val booleadReader: Reader[Boolean] = new Reader[Boolean] {
      override def defaultChoices: Option[BiMap[String, Boolean]] =
        Some(choicesToMap(Vector(true, false)))

      def read: Option[Boolean] = {
        io.StdIn.readLine().trim match {
          case null | ""                  => None
          case "true" | "t" | "yes" | "y" => Some(true)
          case _                          => Some(false)
        }
      }
    }

    implicit val charReader: Reader[Char] = new Reader[Char] {
      def read: Option[Char] = {
        try {
          Some(io.StdIn.readChar())
        } catch {
          case _: EOFException                    => None
          case e: StringIndexOutOfBoundsException => throw new InputException(e)
        }
      }
    }

    implicit val doubleReader: Reader[Double] = new Reader[Double] {
      def read: Option[Double] = {
        try {
          Some(io.StdIn.readDouble())
        } catch {
          case _: EOFException          => None
          case e: NumberFormatException => throw new InputException(e)
        }
      }
    }

    implicit val intReader: Reader[Int] = new IntReader()
  }
}
