package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.Logger

class DxFileTest extends AnyFlatSpec with Matchers {
  val dxApi: DxApi = DxApi(Logger.Quiet)
  val TEST_PROJECT: DxProject = dxApi.project(
      "project-FGpfqjQ0ffPF1Q106JYP2j3v"
  ) // dxWDL_playground
  val PUBLIC_PROJECT: DxProject = dxApi.project("project-FQ7BqkQ0FyXgJxGP2Bpfv3vK") // dxWDL_CI
  val FILE_IN_TWO_PROJS: DxFile = dxApi.file("file-FqPbPZ00ffPG5zf6FvGJyfVp", Some(TEST_PROJECT))
  val FILE_IN_TWO_PROJS_WO_PROJ: DxFile = dxApi.file("file-FqPbPZ00ffPG5zf6FvGJyfVp", None)
  val FILE1: DxFile = dxApi.file("file-FGqFGBQ0ffPPkYP19gBvFkZy", Some(TEST_PROJECT))
  val FILE2: DxFile = dxApi.file("file-FGqFJ8Q0ffPGVz3zGy4FK02P", Some(TEST_PROJECT))
  val FILE3: DxFile = dxApi.file("file-FGzzpkQ0ffPJX74548Vp6670", Some(TEST_PROJECT))
  val FILE4: DxFile = dxApi.file("file-FqP0x4Q0bxKXBBXX5pjVYf3Q", Some(PUBLIC_PROJECT))
  val FILE5: DxFile = dxApi.file("file-FqP0x4Q0bxKykykX5pVXB1YZ", Some(PUBLIC_PROJECT))
  val FILE6: DxFile = dxApi.file("file-FqP0x4Q0bxKykfF55qk98vYj", Some(PUBLIC_PROJECT))
  val FILE6_WO_PROJ: DxFile = dxApi.file("file-FqP0x4Q0bxKykfF55qk98vYj", None)
  val FILE7: DxFile = dxApi.file("file-FqP0x4Q0bxKxJfBb5p90jzKx", Some(PUBLIC_PROJECT))
  val FILE7_WO_PROJ: DxFile = dxApi.file("file-FqP0x4Q0bxKxJfBb5p90jzKx", None)

  it should "bulk describe DxFiles with one project" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE1, FILE2, FILE3))
    result.size shouldBe 3
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE1.id).describe().name shouldBe "fileA"
    result(FILE2.id).describe().name shouldBe "fileB"
    result(FILE3.id).describe().name shouldBe "fileC"
  }

  it should "bulk describe a file without project" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE7_WO_PROJ))
    result.size shouldBe 1
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE7.id).describe().name shouldBe "test4.test"
    result(FILE7.id).describe().project shouldBe PUBLIC_PROJECT.getId
  }

  it should "bulk describe an empty vector" in {
    val result = dxApi.fileBulkDescribe(Vector.empty)
    result.size shouldBe 0
  }

  it should "bulk describe a duplicate file in vector" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE1, FILE2, FILE1))
    result.size shouldBe 2
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE1.id).describe().name shouldBe "fileA"
    result(FILE2.id).describe().name shouldBe "fileB"
  }

  it should "bulk describe a duplicate file in vector2" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE6, FILE6_WO_PROJ))
    result.size shouldBe 1
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE6.id).describe().name shouldBe "test3.test"
  }

  it should "bulk describe a files from multiple projects" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE1, FILE2, FILE5))
    result.size shouldBe 3
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE1.id).describe().name shouldBe "fileA"
    result(FILE1.id).describe().project shouldBe TEST_PROJECT.getId
    result(FILE2.id).describe().name shouldBe "fileB"
    result(FILE2.id).describe().project shouldBe TEST_PROJECT.getId
    result(FILE5.id).describe().name shouldBe "test2.test"
    result(FILE5.id).describe().project shouldBe PUBLIC_PROJECT.getId
  }

  it should "bulk describe a files with and without project" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE4, FILE6_WO_PROJ, FILE7_WO_PROJ))
    result.size shouldBe 3
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE4.id).describe().name shouldBe "test1.test"
    result(FILE4.id).describe().project shouldBe PUBLIC_PROJECT.getId
    result(FILE6.id).describe().name shouldBe "test3.test"
    result(FILE6.id).describe().project shouldBe PUBLIC_PROJECT.getId
    result(FILE7.id).describe().name shouldBe "test4.test"
    result(FILE7.id).describe().project shouldBe PUBLIC_PROJECT.getId
  }

  it should "describe files in bulk with extrafields" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS, FILE2), Set(Field.Details))
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE_IN_TWO_PROJS.id).describe().details shouldBe Some(
        "{\"detail1\":\"value1\"}".parseJson
    )
    result(FILE2.id).describe().details shouldBe Some("{}".parseJson)
  }

  it should "Describe files in bulk without extrafield values - details value should be none" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS, FILE2))
    result.values.forall(_.hasCachedDesc) shouldBe true
    result(FILE_IN_TWO_PROJS.id).describe().details shouldBe None
    result(FILE2.id).describe().details shouldBe None
  }

  it should "bulk describe file which is in two projects, but projects where to search is given" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS))
    result.values.forall(_.hasCachedDesc) shouldBe true
    result.size shouldBe 1
  }

  it should "bulk describe file which is in two projects, project where to search is not given" in {
    val result = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS_WO_PROJ))
    result.values.forall(_.hasCachedDesc) shouldBe true
    result.size shouldBe 2
  }
}
