package dxWDL.dx

import org.scalatest.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import spray.json._

class DxFileTest extends AnyFlatSpec with Matchers {

  val TEST_PROJECT: DxProject = DxProject("project-FGpfqjQ0ffPF1Q106JYP2j3v") // dxWDL_playground
  val PUBLIC_PROJECT: DxProject = DxProject("project-FQ7BqkQ0FyXgJxGP2Bpfv3vK") // dxWDL_CI
  val FILE_IN_TWO_PROJS: DxFile = DxFile("file-FqPbPZ00ffPG5zf6FvGJyfVp", Some(TEST_PROJECT))
  val FILE_IN_TWO_PROJS_WO_PROJ: DxFile = DxFile("file-FqPbPZ00ffPG5zf6FvGJyfVp", None)
  val FILE1: DxFile = DxFile("file-FGqFGBQ0ffPPkYP19gBvFkZy", Some(TEST_PROJECT))
  val FILE2: DxFile = DxFile("file-FGqFJ8Q0ffPGVz3zGy4FK02P", Some(TEST_PROJECT))
  val FILE3: DxFile = DxFile("file-FGzzpkQ0ffPJX74548Vp6670", Some(TEST_PROJECT))
  val FILE4: DxFile = DxFile("file-FqP0x4Q0bxKXBBXX5pjVYf3Q", Some(PUBLIC_PROJECT))
  val FILE5: DxFile = DxFile("file-FqP0x4Q0bxKykykX5pVXB1YZ", Some(PUBLIC_PROJECT))
  val FILE6: DxFile = DxFile("file-FqP0x4Q0bxKykfF55qk98vYj", Some(PUBLIC_PROJECT))
  val FILE6_WO_PROJ: DxFile = DxFile("file-FqP0x4Q0bxKykfF55qk98vYj", None)
  val FILE7: DxFile = DxFile("file-FqP0x4Q0bxKxJfBb5p90jzKx", Some(PUBLIC_PROJECT))
  val FILE7_WO_PROJ: DxFile = DxFile("file-FqP0x4Q0bxKxJfBb5p90jzKx", None)

  it should "bulk describe DxFiles with one project" in {
    val result = DxFile.bulkDescribe(Vector(FILE1, FILE2, FILE3))
    result.size shouldBe 3
    result(FILE1).name shouldBe "fileA"
    result(FILE2).name shouldBe "fileB"
    result(FILE3).name shouldBe "fileC"
  }

  it should "bulk describe a file without project" in {
    val result = DxFile.bulkDescribe(Vector(FILE7_WO_PROJ))
    result.size shouldBe 1
    result(FILE7).name shouldBe "test4.test"
    result(FILE7).project shouldBe PUBLIC_PROJECT.getId
  }

  it should "bulk describe an empty vector" in {
    val result = DxFile.bulkDescribe(Vector.empty)
    result.size shouldBe 0
  }

  it should "bulk describe a duplicate file in vector" in {
    val result = DxFile.bulkDescribe(Vector(FILE1, FILE2, FILE1))
    result.size shouldBe 2
    result(FILE1).name shouldBe "fileA"
    result(FILE2).name shouldBe "fileB"
  }

  it should "bulk describe a duplicate file in vector2" in {
    val result = DxFile.bulkDescribe(Vector(FILE6, FILE6_WO_PROJ))
    result.size shouldBe 1
    result(FILE6).name shouldBe "test3.test"
  }

  it should "bulk describe a files from multiple projects" in {
    val result = DxFile.bulkDescribe(Vector(FILE1, FILE2, FILE5))
    result.size shouldBe 3
    result(FILE1).name shouldBe "fileA"
    result(FILE1).project shouldBe TEST_PROJECT.getId
    result(FILE2).name shouldBe "fileB"
    result(FILE2).project shouldBe TEST_PROJECT.getId
    result(FILE5).name shouldBe "test2.test"
    result(FILE5).project shouldBe PUBLIC_PROJECT.getId
  }

  it should "bulk describe a files with and without project" in {
    val result = DxFile.bulkDescribe(Vector(FILE4, FILE6_WO_PROJ, FILE7_WO_PROJ))
    result.size shouldBe 3
    result(FILE4).name shouldBe "test1.test"
    result(FILE4).project shouldBe PUBLIC_PROJECT.getId
    result(FILE6).name shouldBe "test3.test"
    result(FILE6).project shouldBe PUBLIC_PROJECT.getId
    result(FILE7).name shouldBe "test4.test"
    result(FILE7).project shouldBe PUBLIC_PROJECT.getId
  }

  it should "describe files in bulk with extrafields" in {
    val result = DxFile.bulkDescribe(Vector(FILE_IN_TWO_PROJS, FILE2), Set(Field.Details))
    result(FILE_IN_TWO_PROJS).details shouldBe Some("{\"detail1\":\"value1\"}".parseJson)
    result(FILE2).details shouldBe Some("{}".parseJson)
  }

  it should "Describe files in bulk without extrafield values - details value should be none" in {
    val result = DxFile.bulkDescribe(Vector(FILE_IN_TWO_PROJS, FILE2))
    result(FILE_IN_TWO_PROJS).details shouldBe None
    result(FILE2).details shouldBe None
  }

  it should "bulk describe file which is in two projects, but projects where to search is given" in {
    val result = DxFile.bulkDescribe(Vector(FILE_IN_TWO_PROJS))
    result.size shouldBe 1
  }

  it should "bulk describe file which is in two projects, project where to search is not given" in {
    val result = DxFile.bulkDescribe(Vector(FILE_IN_TWO_PROJS_WO_PROJ))
    result.size shouldBe 2
  }
}
