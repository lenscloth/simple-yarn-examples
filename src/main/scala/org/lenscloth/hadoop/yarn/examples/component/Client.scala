package org.lenscloth.hadoop.yarn.examples.component

import java.io.File

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.lenscloth.hadoop.yarn.examples.constant.{ApplicationSubmissionConstant, ContainerLaunchConstant}
import org.lenscloth.hadoop.yarn.examples.utils.{ContainerLaunchUtils, HDFSUtils, SecurityUtils}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import scala.collection.JavaConverters._

class Client {
  private val yarnClient = YarnClient.createYarnClient()
  private val LOG = LogFactory.getLog(classOf[Client])

  /** Get configuration of yarn that you will submit */
  private val conf = new YarnConfiguration()

  /** You must set fs.defaultFS to acquire HDFS */
  private val hdfs = FileSystem.get(conf)

  /** initialize yarnClient */
  yarnClient.init(conf)
  yarnClient.start()

  /**
    *
    * @param name appName
    * @param resources localResources that will be loaded on the AppMaster container
    * @param env Environment variable for AppMaster
    * @param appMasterCMD Commands to launch AppMaster
    * @param priority priority of the submitted application
    * @param queue queue that application will be submitted
    *
    * In order to submit an application on yarn,
    * You need to initialize an applicationSubmissionContext
    * and submit that applicationSubmissionContext to yarnClient
    */
  def newApp(name: String,
             resources: List[String],
             env: Map[String, String],
             appMasterCMD: List[String],
             priority: Priority,
             queue: String,
             stagingDir: Path): ApplicationSubmissionContext = {
    val newApp = yarnClient.createApplication()
    val appSubmissionContext = newApp.getApplicationSubmissionContext

    val containerResources = HDFSUtils.loadLocalResources(hdfs, stagingDir, resources)

    /** Envs for AppMaster */
    val containerEnv = ContainerLaunchUtils.appendClassPath(env, conf)

    /** Delegation token that has permission to access HDFS */
    val cred = new Credentials()
    SecurityUtils.loadHDFSCredential(hdfs, conf, cred)
    val tokens = SecurityUtils.wrapToByteBuffer(cred)

    val amContainerLaunchContext = ContainerLaunchContext.newInstance(containerResources.asJava, containerEnv.asJava, appMasterCMD.asJava, null, tokens, null)

    /** Memory and CPU that will be allocated for app master */
    val resource = Resource.newInstance(ContainerLaunchConstant.defaultMemory, ContainerLaunchConstant.defaultCore)

    /** Set applicationSubmissionContext **/
    appSubmissionContext.setApplicationName(name)
    appSubmissionContext.setApplicationType("example")
    appSubmissionContext.setPriority(priority)
    appSubmissionContext.setQueue(queue)
    appSubmissionContext.setResource(resource)

    /** Set containerLaunchContext */
    appSubmissionContext.setAMContainerSpec(amContainerLaunchContext)

    /** Attempt application submission 3 times util success */
    appSubmissionContext.setMaxAppAttempts(ApplicationSubmissionConstant.defaultMaxAttempt)
    appSubmissionContext.setAttemptFailuresValidityInterval(ApplicationSubmissionConstant.defaultAttemptFailureValidityInterval)

    /** Even if application submission fail, Container should be kept
      * and its local resources should be remained on that container
      *
      * The container will be used again to attempt application submission
      */
    appSubmissionContext.setKeepContainersAcrossApplicationAttempts(ApplicationSubmissionConstant.defaultKeepContainerAcrossApplicationAttempts)
    appSubmissionContext
  }

  def submitApp(applicationSubmissionContext: ApplicationSubmissionContext, timeout: Option[Long]): Unit = {
    val appId = applicationSubmissionContext.getApplicationId
    val submitTime = System.currentTimeMillis()

    yarnClient.submitApplication(applicationSubmissionContext)

    def recReport(): Unit = {
      Thread.sleep(5000)
      val report = yarnClient.getApplicationReport(appId)
      val state = stateApplicationReport(report)

      state match {
        case YarnApplicationState.FAILED | YarnApplicationState.FINISHED | YarnApplicationState.KILLED => /** stop reporting **/
        case _ =>
          /** If timeout is not specified then run report recursively Else run report only before timeout */
         def rec: () => Unit = timeout.map (limit =>
           () => { if(limit < System.currentTimeMillis() - submitTime) recReport() }
           ).getOrElse(recReport)
         rec()
      }
    }

    recReport()
  }

  private def stateApplicationReport(report: ApplicationReport): YarnApplicationState = {
    LOG.info("Got application report from ASM for" + ", appId=" + report.getApplicationId +
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
        LOG.info("Application has completed successfully")
      else
        LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString)
    }
    else if ((YarnApplicationState.KILLED eq state) || (YarnApplicationState.FAILED eq state)) {
      LOG.info("Application did not finish." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString)
    }

    state
  }
}
