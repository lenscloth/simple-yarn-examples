package org.lenscloth.hadoop.yarn.examples.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType, LocalResourceVisibility}
import org.apache.hadoop.yarn.util.ConverterUtils

object HDFSUtils {
  def loadLocalResources(fs: FileSystem, stagingDirectory: Path ,files: List[String]): Map[String, LocalResource] =
    files.map { file =>
      val dstPath = new Path(stagingDirectory, file)
      /** Load local file to HDFS **/
      fs.copyFromLocalFile(new Path(file), dstPath)
      (file, toLocalResource(fs, stagingDirectory, file))
    }.toMap

  def toLocalResource(fs: FileSystem, stagingDirectory: Path, file: String): LocalResource = {
    val dstPath = fs.resolvePath(new Path(stagingDirectory, file))
    val fileStatus = fs.getFileStatus(dstPath)
    LocalResource.newInstance(
      ConverterUtils.getYarnUrlFromURI(dstPath.toUri),
      LocalResourceType.FILE,
      LocalResourceVisibility.PUBLIC,
      fileStatus.getLen,
      fileStatus.getModificationTime)
  }
}
