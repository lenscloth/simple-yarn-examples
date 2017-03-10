package org.lenscloth.hadoop.yarn.examples.component

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.constant.ContainerLaunchConstant
import org.lenscloth.hadoop.yarn.examples.utils.{ContainerLaunchUtils, HDFSUtils, SecurityUtils}

import scala.collection.JavaConverters._

object ApplicationMaster {
  def main(args: Array[String]): Unit = {
    val containerCMD = args(0).split(" ").toList
    val stagingDir = new Path(args(1))
    val nTimes = args(2).toInt
    val stagedResources = args(3).split(" ").toList

    val appMaster = new ApplicationMaster (
      containerCMD,
      stagingDir,
      stagedResources
    )

    appMaster.run(nTimes)
  }
}

/**
  * This application master launches multiple container that runs same process
  */
class ApplicationMaster(containerCMD: List[String], stagingDir: Path, stagedResources: List[String]) {
  private val envs = ContainerLaunchUtils.appendClassPath(Map.empty[String, String], conf)
  private val conf = new YarnConfiguration()
  private val hdfs: FileSystem = FileSystem.get(conf)

  private val LOG = LogFactory.getLog(classOf[ApplicationMaster])

  /** For simple demonstration, all containers (AM, or other containers) have same localResources */
  private val containerResources: Map[String, LocalResource] =
    stagedResources.map { r =>
      (r, HDFSUtils.toLocalResource(hdfs, stagingDir, r))}.toMap

  private val amRMClient: AMRMClient[AMRMClient.ContainerRequest] = AMRMClient.createAMRMClient()
  private val nmClient: NMClient = NMClient.createNMClient()

  amRMClient.init(conf)
  amRMClient.start()
  amRMClient.registerApplicationMaster("", 0, "")

  nmClient.init(conf)
  nmClient.start()

  def run(n: Int): Unit = {
    val resource = Resource.newInstance(ContainerLaunchConstant.defaultMemory, ContainerLaunchConstant.defaultCore)
    val priority = Priority.newInstance(0)

    val cred = new Credentials()
    SecurityUtils.loadHDFSCredential(hdfs, conf, cred)
    val tokens = SecurityUtils.wrapToByteBuffer(cred)


    val containerLaunchContext =
      ContainerLaunchContext.newInstance(containerResources.asJava, envs.asJava ,containerCMD.asJava, null, tokens, null)

    (1 to n).foreach { _ => amRMClient.addContainerRequest(new ContainerRequest(resource, null, null, priority)) }

    LOG.info(s"Application Master is now launching $n containers")

    def launchAndCheckProgress(completedContainers: Int): Unit = {
      if(completedContainers < n) {
        val response = amRMClient.allocate(completedContainers/n.toFloat)
        response.getAllocatedContainers.asScala.foreach { container =>
          LOG.info(s"Container ${container.getId.toString} started with command(${containerCMD.mkString(" ")})")
          nmClient.startContainer(container, containerLaunchContext)
        }

        val newCompletedContainers = response.getCompletedContainersStatuses
        newCompletedContainers.asScala.foreach { container =>
          LOG.info(s"Container ${container.getContainerId} is completed with status ${container.getDiagnostics}")
        }

        launchAndCheckProgress(completedContainers + newCompletedContainers.size())
      }
    }

    launchAndCheckProgress(0)
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
  }
}
