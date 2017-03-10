package org.lenscloth.hadoop.yarn.examples

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.component.Client
import org.lenscloth.hadoop.yarn.examples.constant.ApplicationSubmissionConstant

object flat {
  def main(args: Array[String]): Unit = {
    val name = "flat"
    val yarnClient = new Client()

    val conf = new YarnConfiguration()

    val priority = Priority.newInstance(0)
    val queue = ApplicationSubmissionConstant.defaultQueue
    val path = FileSystem.get(conf).getHomeDirectory

    val fileToFlat = args(0)
    val nTimes = args(1)
    val localResources = args(2).split(" ").toList

    val programCMD = s"java org.lenscloth.hadoop.yarn.examples.functions.flatter $fileToFlat > ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout"
    val cmd = List("java", "org.lenscloth.hadoop.yarn.examples.component.ApplicationMaster", programCMD, path.toString, nTimes, localResources.mkString(" "), ">", s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout")

    val app =
      yarnClient.newApp(
        name,
        localResources,
        Map.empty[String, String],
        cmd,
        priority,
        queue,
        path
      )

    yarnClient.submitApp(app, Some(60 * 1000))
  }
}
