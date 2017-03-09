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

      val fileStatus = fs.getFileStatus(dstPath)
      val localResource =
        LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromURI(dstPath.toUri),
          LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION,
          fileStatus.getLen,
          fileStatus.getModificationTime)

      (file, localResource)
    }.toMap

}
