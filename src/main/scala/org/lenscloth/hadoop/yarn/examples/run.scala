package org.lenscloth.hadoop.yarn.examples

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{LocalResource, Priority}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.component.Client
import org.lenscloth.hadoop.yarn.examples.constant.ApplicationSubmissionConstant
import org.lenscloth.hadoop.yarn.examples.utils.{CLI, HDFSUtils, SecurityUtils}

object run {
  def main(args: Array[String]): Unit = {
    val optConfig = CLI.parse(args.toList)

    val yarnClient = new Client()
    val conf = new YarnConfiguration()

    val hdfs = FileSystem.get(conf)
    val priority = Priority.newInstance(0)
    val queue = ApplicationSubmissionConstant.defaultQueue

    optConfig.foreach { config =>
      val name = config.name
      val resources = config.resources.toList
      val stagingDir = config.stagingDir
      val command = config.command.split(" ").toList ++ List("1>", s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout", "2>", s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr")

      val envs = config.envs ++ Map(("STAGING_DIR", stagingDir), ("RESOURCES", resources.mkString(" ")))

      val appMasterResources = HDFSUtils.loadLocalResources(hdfs, new Path(stagingDir), resources)

      println(appMasterResources)

      val credential = new Credentials()
      SecurityUtils.loadHDFSCredential(hdfs, conf, credential)

      val app =
        yarnClient.newApp(
          name,
          appMasterResources,
          envs,
          command,
          priority,
          queue,
          credential
        )

      yarnClient.submitApp(app, Some(60 * 1000))
    }

  }
}
