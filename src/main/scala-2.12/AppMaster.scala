import java.nio.ByteBuffer

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import scala.collection.JavaConverters._

object AppMaster {
  def main(args: Array[String]): Unit = {
    val am = new AppMaster
    am.run()
  }
}

class AppMaster {
  private val LOG = LogFactory.getLog(classOf[AppMaster])

  val conf = new YarnConfiguration()
  private val envs = System.getenv()
  private val cmmds = envs.get("PROGRAM_COMMANDS").split(" ")
  private val hdfs = FileSystem.get(conf)

  private val resources =  {
    val programResources = envs.get("PROGRAM_RESOURCES").split(" ")
    programResources.map { resource =>
      val p = PathUtils.toHDFSHome(hdfs, resource)
      val fileStatus = hdfs.getFileStatus(p)
      (resource, LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(p.toUri),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        fileStatus.getLen,
        fileStatus.getModificationTime))
    }.toMap
  }

  // Configuration
  val containerMemory = 10
  val containerVirtualCores = 1
  val requestPriority = 0

  // Initialize clients to ResourceManager and NodeManagers
  val amRMClient: AMRMClient[AMRMClient.ContainerRequest] = AMRMClient.createAMRMClient()
  amRMClient.init(conf)
  amRMClient.start()

  // Register with ResourceManager ?? Is this empty??
  amRMClient.registerApplicationMaster("", 0, "")

  val nmClient: NMClient = NMClient.createNMClient
  nmClient.init(conf)
  nmClient.start()

  // Set up resource type requirements for Container
  val capability: Resource = Resource.newInstance(containerMemory, containerVirtualCores)

  // Priority for worker containers - priorities are intra-application
  val priority: Priority = Priority.newInstance(requestPriority)

  val credentials = new Credentials()
  val tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL)
  hdfs.addDelegationTokens(tokenRenewer, credentials)
  val dob = new DataOutputBuffer()
  credentials.writeTokenStorageToStream(dob)

  val tokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)

  val containerLaunchContext: ContainerLaunchContext =
    ContainerLaunchContext.newInstance(resources.asJava, Map.empty[String, String].asJava, cmmds.toList.asJava, null, null, null)
  containerLaunchContext.setTokens(tokens)

  def run(): Unit = {
    amRMClient.addContainerRequest(new AMRMClient.ContainerRequest(capability, null, null, priority))
    val response = amRMClient.allocate(0)

    val container = response.getAllocatedContainers.get(0)
    nmClient.startContainer(container, containerLaunchContext)

    def checkProgress(): Unit = {
      val response = amRMClient.allocate(0)
      val finishedContainers = response.getCompletedContainersStatuses

      if (finishedContainers.isEmpty) {
        Thread.sleep(1000)
        checkProgress()
      } else {
        val status = finishedContainers.get(0)
        LOG.info("ContainerID:" + status.getContainerId + ", state:" + status.getState.name)
      }
    }

    checkProgress()
    // Un-register with ResourceManager
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
    LOG.info("Finished MyApplicationMaster")
  }
}