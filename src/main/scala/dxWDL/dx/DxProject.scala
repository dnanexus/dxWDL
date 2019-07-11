package dxWDL.dx

import com.dnanexus.DXDataObject
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
//import java.nio.file.{Path, Files}
import spray.json._

case class FolderContents(dataObjects: List[DXDataObject],
                          subfolders : List[String])

case class DxProject(id: String) {
    def getId : String = id

    def listFolder(path : String) : FolderContents = ???

    def describe : DxDescribe = ???

    def newFolder(folderPath : String, parents : Boolean) : Unit = ???

    def move(files: List[DXDataObject], destinationFolder : String) : Unit = ???

}


object DxProject {
    def getInstance(id : String) = DxProject(id)
}
