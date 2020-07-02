package dx.api

import dx.core.io.DxFileCache
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

  private def checkFileDesc(query: Vector[DxFile],
                            expected: Vector[DxFile],
                            expectedSize: Option[Int] = None,
                            compareDetails: Boolean = false): Unit = {
    val extraArgs = if (compareDetails) Set(Field.Details) else Set.empty[Field.Value]
    val result = dxApi.fileBulkDescribe(query, extraArgs)
    result.size shouldBe expectedSize.getOrElse(expected.size)
    result.forall(_.hasCachedDesc) shouldBe true
    val lookup = DxFileCache(expected)
    result.foreach { r =>
      val e = lookup.getCached(r)
      e shouldBe defined
      r.describe().name shouldBe e.get.describe().name
      r.project shouldBe e.get.project
      if (compareDetails) {
        r.describe().details shouldBe e.get.describe().details
      }
    }
  }

  def createFile(template: DxFile,
                 name: String,
                 project: Option[DxProject] = None,
                 details: Option[String] = None): DxFile = {
    val proj = project
      .orElse(template.project)
      .getOrElse(
          throw new Exception("no project")
      )
    val file = DxFile(dxApi, template.id, Some(proj))
    val desc = DxFileDescribe(
        proj.id,
        template.id,
        name,
        null,
        0,
        0,
        0,
        null,
        null,
        details.map(_.parseJson),
        null
    )
    file.cacheDescribe(desc)
    file
  }

  def createFiles(templates: Vector[DxFile],
                  names: Vector[String],
                  projects: Vector[DxProject] = Vector.empty): Vector[DxFile] = {
    templates.zip(names).zipWithIndex.map {
      case ((template, name), i) =>
        val project = if (projects.isEmpty) {
          None
        } else if (projects.size <= i) {
          Some(projects.head)
        } else {
          Some(projects(i))
        }
        createFile(template, name, project)
    }
  }

  it should "bulk describe DxFiles with one project" in {
    val query = Vector(FILE1, FILE2, FILE3)
    checkFileDesc(query, createFiles(query, Vector("fileA", "fileB", "fileC")))
  }

  it should "bulk describe a file without project" in {
    val query = Vector(FILE7_WO_PROJ)
    checkFileDesc(query, createFiles(query, Vector("test4.test"), Vector(PUBLIC_PROJECT)))
  }

  it should "bulk describe an empty vector" in {
    val result = dxApi.fileBulkDescribe(Vector.empty)
    result.size shouldBe 0
  }

  it should "bulk describe a duplicate file in vector" in {
    checkFileDesc(Vector(FILE1, FILE2, FILE1),
                  createFiles(Vector(FILE1, FILE2), Vector("fileA", "fileB")))
  }

  it should "bulk describe a duplicate file in vector2" in {
    checkFileDesc(Vector(FILE6, FILE6_WO_PROJ),
                  Vector(createFile(FILE6, "test3.test")),
                  expectedSize = Some(2))
  }

  it should "bulk describe a files from multiple projects" in {
    val query = Vector(FILE1, FILE2, FILE5)
    checkFileDesc(query, createFiles(query, Vector("fileA", "fileB", "test2.test")))
  }

  it should "bulk describe a files with and without project" in {
    val query = Vector(FILE4, FILE6_WO_PROJ, FILE7_WO_PROJ)
    checkFileDesc(query,
                  createFiles(query,
                              Vector("test1.test", "test3.test", "test4.test"),
                              Vector(PUBLIC_PROJECT, PUBLIC_PROJECT, PUBLIC_PROJECT)))
  }

  it should "describe files in bulk with extrafields" in {
    val expected = Vector(
        createFile(FILE_IN_TWO_PROJS,
                   "File_copied_to_another_project",
                   Some(TEST_PROJECT),
                   Some("{\"detail1\":\"value1\"}")),
        createFile(FILE2, "fileB", Some(TEST_PROJECT), Some("{}"))
    )
    checkFileDesc(Vector(FILE_IN_TWO_PROJS, FILE2), expected, compareDetails = true)
  }

  it should "Describe files in bulk without extrafield values - details value should be none" in {
    val results = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS, FILE2))
    results.foreach(f => f.describe().details shouldBe None)
  }

  it should "bulk describe file which is in two projects, but projects where to search is given" in {
    val results = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS))
    results.forall(_.hasCachedDesc) shouldBe true
    results.size shouldBe 1
  }

  it should "bulk describe file which is in two projects, project where to search is not given" in {
    val results = dxApi.fileBulkDescribe(Vector(FILE_IN_TWO_PROJS_WO_PROJ))
    results.forall(_.hasCachedDesc) shouldBe true
    results.size shouldBe 2
  }
}
