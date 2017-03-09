package org.lenscloth.hadoop.yarn.examples

import component.Client
import org.apache.hadoop.yarn.api.records.Priority
import org.lenscloth.hadoop.yarn.examples.constant.ApplicationSubmissionConstant
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration

object echo {
  def main(args: Array[String]): Unit = {
    val name = "echo"
    val yarnClient = new Client()

    val conf = new YarnConfiguration()

    val priority = Priority.newInstance(0)
    val queue = ApplicationSubmissionConstant.defaultQueue
    val path = FileSystem.get(conf).getHomeDirectory

    val app =
      yarnClient.newApp(
        name,
        List.empty[String],
        Map.empty[String, String],
        List("echo", args.mkString(" ")),
        priority,
        queue,
        path
      )

    yarnClient.submitApp(app)
  }
}
