package dx.api

import com.dnanexus.AccessLevel

sealed abstract class DxAccessLevel(javaLevel: AccessLevel, val order: Int)
    extends Ordered[DxAccessLevel] {
  def compare(that: DxAccessLevel): Int = this.order - that.order
  lazy val name: String = javaLevel.name()
}

object DxAccessLevel {
  case object Denied extends DxAccessLevel(AccessLevel.NONE, 1)
  case object View extends DxAccessLevel(AccessLevel.VIEW, 2)
  case object Upload extends DxAccessLevel(AccessLevel.UPLOAD, 3)
  case object Contribute extends DxAccessLevel(AccessLevel.CONTRIBUTE, 4)
  case object Administer extends DxAccessLevel(AccessLevel.ADMINISTER, 5)
  val All: Vector[DxAccessLevel] =
    Vector(Denied, View, Upload, Contribute, Administer).sortWith(_ < _)

  def withName(name: String): DxAccessLevel = {
    All.find(_.name == name) match {
      case Some(level) => level
      case _           => throw new NoSuchElementException(s"No value found for ${name}")
    }
  }

  def withOrder(order: Int): DxAccessLevel = {
    All.find(_.order == order) match {
      case Some(level) => level
      case _           => throw new NoSuchElementException(s"No value found for ${order}")
    }
  }
}
