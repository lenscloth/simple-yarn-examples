package org.lenscloth.hadoop.yarn.examples.component

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.constant.ContainerLaunchConstant
import org.lenscloth.hadoop.yarn.examples.utils.{ContainerLaunchUtils, HDFSUtils, SecurityUtils}

import scala.collection.JavaConverters._

object Spreader {
  def main(args: Array[String]): Unit = {
    val nTimes = args(0).toInt
    val containerCMD = args.tail.toList

    val stagingDir = new Path(System.getenv("STAGING_DIR"))
    val stagedResources = System.getenv("RESOURCES").split(" ").toList

    val appMaster = new Spreader (
      containerCMD,
      stagingDir,
      stagedResources
    )

    appMaster.spread(nTimes)
  }
}

/**
  * This application master launches multiple container that runs same process
  */
class Spreader(containerCMD: List[String], stagingDir: Path, stagedResources: List[String]) {
  private val conf = new YarnConfiguration()
  private val envs = ContainerLaunchUtils.appendYarnClassPath(Map.empty[String, String], conf, stagedResources)
  private val hdfs: FileSystem = FileSystem.get(conf)

  private val LOG = LogFactory.getLog(classOf[Spreader])

  /** For simple demonstration, all containers (AM, or other containers) have same localResources */
  private val containerResources: Map[String, LocalResource] = stagedResources.map( r => (r, HDFSUtils.toLocalResource(hdfs, stagingDir, r))).toMap

  private val amRMClient: AMRMClient[AMRMClient.ContainerRequest] = AMRMClient.createAMRMClient()
  private val nmClient: NMClient = NMClient.createNMClient()

  amRMClient.init(conf)
  amRMClient.start()
  amRMClient.registerApplicationMaster("", 0, "")

  nmClient.init(conf)
  nmClient.start()

  def spread(n: Int): Unit = {
    val resource = Resource.newInstance(ContainerLaunchConstant.defaultMemory, ContainerLaunchConstant.defaultCore)
    val priority = Priority.newInstance(0)

    val cred = UserGroupInformation.getCurrentUser.getCredentials
    val tokens = SecurityUtils.wrapToByteBuffer(cred)

    (1 to n).foreach { _ =>
      amRMClient.addContainerRequest(new ContainerRequest(resource, null, null, priority))
    }

    LOG.info(s"Application Master is now launching $n containers")

    def launchAndCheckProgress(completedContainers: Int): Unit = {
      if(completedContainers < n) {
        val response = amRMClient.allocate(completedContainers/n.toFloat)
        response.getAllocatedContainers.asScala.foreach { container =>
          val containerLaunchContext =
            ContainerLaunchContext.newInstance(
              containerResources.asJava,
              envs.asJava ,
              (containerCMD ++ List("1>",s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout","2>",s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr")).asJava,
              null, tokens, null)

          LOG.info(s"Container ${container.getId.toString} started with command(${containerCMD.mkString(" ")})")
          nmClient.startContainer(container, containerLaunchContext)
        }

        val newCompletedContainers = response.getCompletedContainersStatuses
        newCompletedContainers.asScala.foreach { container =>
          LOG.info(s"Container ${container.getContainerId} is completed with exit status number ${container.getExitStatus}")
        }

        launchAndCheckProgress(completedContainers + newCompletedContainers.size())
      }
    }

    launchAndCheckProgress(0)
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
  }
}
