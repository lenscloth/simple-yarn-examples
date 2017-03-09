package org.lenscloth.hadoop.yarn.examples.component

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.constant.ApplicationMasterConstant
import org.lenscloth.hadoop.yarn.examples.utils.HDFSUtils

class ApplicationMaster {
  private val envs = System.getenv()
  private val stagingDir: Path = new Path(envs.get(ApplicationMasterConstant.STAGING_DIR))
  private val conf = new YarnConfiguration()
  private val hdfs: FileSystem = FileSystem.get(conf)

  private val containerResources: Map[String, LocalResource] = {
    val resources =
      envs.get(ApplicationMasterConstant.CONTAINER_LOCAL_RESOURCE).split(" ").toList
    resources.map { r =>
      (r, HDFSUtils.toLocalResource(hdfs, stagingDir, r))}.toMap
  }

  private val containerCMD: List[String] =
    envs.get(ApplicationMasterConstant.CONTAINER_CMD).split(" ").toList
}
