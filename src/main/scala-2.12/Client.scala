import java.nio.ByteBuffer

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import scala.collection.JavaConverters._

object Client {
  /**
    * Args 0: _.jar file
    * Args 1: "App master command"
    * Args 2: "program command"
    */

  def main(args: Array[String]): Unit = {
    val client = new Client
    val resources = args.head.split(" ").toList

    val masterCommands = args(1).split(" ").toList
    val programCommands = args(2).split(" ").toList

    val envs = Map(
      "PROGRAM_COMMANDS"-> programCommands.mkString(" "),
      "PROGRAM_RESOURCES" -> resources.mkString(" "))

    val priority = Priority.newInstance(0)

    val appContext =
      client.createApp("flatter", resources, envs, masterCommands ,priority, None)

    client.launchApp(appContext)
  }
}

class Client {

  private val yarnClient = YarnClient.createYarnClient()
  private val LOG = LogFactory.getLog(classOf[Client])
  private val conf = new YarnConfiguration()

  private val hdfs = FileSystem.get(conf)
  yarnClient.init(conf)
  yarnClient.start()

  def createApp(appName: String,
                resources: List[String],
                env: Map[String, String],
                commands: List[String],
                priority: Priority,
                queue: Option[String]): ApplicationSubmissionContext = {

    val app = yarnClient.createApplication()
    val credentials = new Credentials()

    val appContext = app.getApplicationSubmissionContext

    appContext.setApplicationName(appName)
    appContext.setKeepContainersAcrossApplicationAttempts(true)
    appContext.setCancelTokensWhenComplete(false)
    appContext.setMaxAppAttempts(3)
    appContext.setPriority(priority)
    appContext.setQueue(queue.getOrElse("default"))

    val resource = Resource.newInstance(10, 1)
    appContext.setResource(resource)

    val localResources =
      resources.map { srcFile =>
        val dstPath = PathUtils.toHDFSHome(hdfs, srcFile)
        hdfs.copyFromLocalFile(new Path(srcFile), dstPath)
        val fileStatus = hdfs.getFileStatus(dstPath)
        val localResource =
          LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dstPath.toUri),
            LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION,
            fileStatus.getLen,
            fileStatus.getModificationTime)

        (srcFile, localResource)
      }.toMap

    val containerLaunchContext = ContainerLaunchContext.newInstance(localResources.asJava, env.asJava, commands.asJava, null, null, null)

    // Set up token
    val tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL)
    hdfs.addDelegationTokens(tokenRenewer, credentials)
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    val tokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
    containerLaunchContext.setTokens(tokens)

    appContext.setAMContainerSpec(containerLaunchContext)
    appContext
  }

  def launchApp(applicationSubmissionContext: ApplicationSubmissionContext): Unit = {
    val appId = applicationSubmissionContext.getApplicationId
    yarnClient.submitApplication(applicationSubmissionContext)

    def recReportApp: Unit = {
      Thread.sleep(1000)
      val report = yarnClient.getApplicationReport(appId)
      LOG.info("Got application report from ASM for" + ", appId=" + appId.getId +
        ", clientToAMToken=" + report.getClientToAMToken +
        ", appDiagnostics=" + report.getDiagnostics +
        ", appMasterHost=" + report.getHost +
        ", appQueue=" + report.getQueue +
        ", appMasterRpcPort=" + report.getRpcPort +
        ", appStartTime=" + report.getStartTime +
        ", yarnAppState=" + report.getYarnApplicationState.toString +
        ", distributedFinalState=" + report.getFinalApplicationStatus.toString +
        ", appTrackingUrl=" + report.getTrackingUrl +
        ", appUser=" + report.getUser)

      val state = report.getYarnApplicationState
      val dsStatus = report.getFinalApplicationStatus

      if (YarnApplicationState.FINISHED eq state) {
        if (FinalApplicationStatus.SUCCEEDED eq dsStatus)
          LOG.info("Application has completed successfully. Breaking monitoring loop")
        else
          LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString + ". Breaking monitoring loop")
      }
      else if ((YarnApplicationState.KILLED eq state) || (YarnApplicationState.FAILED eq state)) {
        LOG.info("Application did not finish." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString + ". Breaking monitoring loop")
      }
      recReportApp
    }

    recReportApp
  }
}