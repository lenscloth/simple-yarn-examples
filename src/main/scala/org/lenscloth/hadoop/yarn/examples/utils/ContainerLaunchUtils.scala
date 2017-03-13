package org.lenscloth.hadoop.yarn.examples.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration

object ContainerLaunchUtils {
  def appendYarnClassPath(env: Map[String, String], conf: Configuration, resources: List[String]): Map[String, String] = {
    val yarnClassPathEnv =
      conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
        .map(e => (Environment.CLASSPATH.name(), e.trim)).toMap
    val containerLocalDirectory = Map((Environment.CLASSPATH.name(), s"${Environment.PWD.$()}${File.separator}*"))
    val resourceClassPath = resources.map ( r => (Environment.CLASSPATH.name(), r.trim)).toMap

    yarnClassPathEnv ++ containerLocalDirectory ++ resourceClassPath ++ env
  }
}
