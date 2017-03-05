import org.apache.commons.logging.LogFactory
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{ AMRMClient, NMClient }
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

class AppMaster {
  val envs = System.getenv()
  private val LOG = LogFactory.getLog(classOf[AppMaster])

  val containerId = {
    val containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID)
    ConverterUtils.toContainerId(containerIdString)
  }
  val appAttemptID = containerId.getApplicationAttemptId
  val conf = new YarnConfiguration()

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

  def launchContainer(containerLaunchContext: ContainerLaunchContext): Unit = {
    amRMClient.addContainerRequest(new AMRMClient.ContainerRequest(capability, null, null, priority))
    val response = amRMClient.allocate(0)

    val container = response.getAllocatedContainers.get(0)
    nmClient.startContainer(container, containerLaunchContext)

    def checkProgress: Unit = {
      val response = amRMClient.allocate(0)
      val finshedContainers = response.getCompletedContainersStatuses

      if (finshedContainers.isEmpty) {
        Thread.sleep(1000)
        checkProgress
      } else {
        val status = finshedContainers.get(0)
        LOG.info("ContainerID:" + status.getContainerId + ", state:" + status.getState.name)
      }
    }

    checkProgress
    // Un-register with ResourceManager
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
    LOG.info("Finished MyApplicationMaster")
  }
}