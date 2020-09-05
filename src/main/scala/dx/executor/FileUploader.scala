package dx.executor

import java.nio.file.Path

import dx.api.{DxApi, DxFile}

trait FileUploader {
  def upload(files: Set[Path]): Map[Path, DxFile]
}

/**
  * Simple FileUploader that uploads one file at a time
  * using the API.
  * @param dxApi DxApi
  */
case class SerialFileUploader(dxApi: DxApi = DxApi.get) extends FileUploader {
  def upload(files: Set[Path]): Map[Path, DxFile] = {
    files.map(path => path -> dxApi.uploadFile(path)).toMap
  }
}

// TODO: implement parallel (probably UA-based) uploader
