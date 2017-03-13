package org.lenscloth.hadoop.yarn.examples.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration

object ContainerLaunchUtils {
  def appendYarnClassPath(env: Map[String, String], conf: Configuration, resources: List[String]): Map[String, String] = {
    val cp1 =
      conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*).mkString(":")

    val cp2 =
      cp1 + ":" + s"${Environment.PWD.$()}${File.separator}*"

    val cp3 =
      cp2 + ":" + resources.mkString(":")

    env ++ Map((Environment.CLASSPATH.name(), cp3))
  }
}
