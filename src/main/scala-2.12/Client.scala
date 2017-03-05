import java.io.IOException
import java.nio.ByteBuffer

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConverters._

object Client {
  def main(args: Array[String]): Unit = {
    val client = new Client

  }
}

class Client {

  private val yarnClient = YarnClient.createYarnClient()
  private val LOG = LogFactory.getLog(classOf[Client])
  private val conf = new YarnConfiguration()

  yarnClient.init(conf)
  yarnClient.start()

  def createApp(appName: String,
    resources: Map[String, LocalResource],
    env: Map[String, String], credentials: Credentials,
    priority: Priority, queue: Option[String]): ApplicationSubmissionContext = {
    val app = yarnClient.createApplication()
    val response = app.getNewApplicationResponse

    val appContext = app.getApplicationSubmissionContext

    appContext.setApplicationName(appName)
    appContext.setKeepContainersAcrossApplicationAttempts(true)
    appContext.setCancelTokensWhenComplete(false)
    appContext.setMaxAppAttempts(3)
    appContext.setPriority(priority)
    appContext.setQueue(queue.getOrElse("default"))
    appContext.setResource(response.getMaximumResourceCapability)

    val containerLaunchContext = ContainerLaunchContext.newInstance(resources.asJava, env.asJava, List.empty[String].asJava, null, null, null)

    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    val tokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
    containerLaunchContext.setTokens(tokens)

    appContext.setAMContainerSpec(containerLaunchContext)
    appContext
  }

  def launchApp(applicationSubmissionContext: ApplicationSubmissionContext): Unit = {
    val appId = applicationSubmissionContext.getApplicationId

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
        recReportApp
      }
      else if ((YarnApplicationState.KILLED eq state) || (YarnApplicationState.FAILED eq state)) {
        LOG.info("Application did not finish." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString + ". Breaking monitoring loop")
      }
    }
    recReportApp
  }

}