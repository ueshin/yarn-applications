/*
 * Copyright 2012 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.yarn.applications.simple

import scala.annotation.tailrec
import scala.collection.JavaConversions._

import org.apache.commons.logging._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.net._
import org.apache.hadoop.util._
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.conf._
import org.apache.hadoop.yarn.ipc._
import org.apache.hadoop.yarn.util._

/**
 * @author ueshin
 *
 */
class SimpleYarnApplicationClient(conf: Configuration = new Configuration) extends Configured(conf) with Tool {

  import SimpleYarnApplicationClient._

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  def run(args: Array[String]) = {
    val fs = FileSystem.get(getConf)

    val rpc = YarnRPC.create(getConf)

    // Connect to ApplicationsManager
    val yarnConf = new YarnConfiguration(getConf)
    val rmAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS))
    val applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf).asInstanceOf[ClientRMProtocol]
    LOG.info("Connecting to ResourceManager at " + rmAddress)

    // Get ApplicationId from ApplicationsManager
    val application = applicationsManager.getNewApplication(Records.newRecord(classOf[GetNewApplicationRequest]))
    LOG.info("Got new ApplicationId=" + application.getApplicationId)

    // Create a new ApplicationSubmissionContext
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(application.getApplicationId)
    appContext.setApplicationName(ApplicationName)

    // Create a new container launch context for the AM's container
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])

    // Define the local resources required
    val localResources = args.map { jar =>
      val jarPath = new Path(jar)
      val jarStatus = fs.getFileStatus(jarPath)
      val amJarRsrc = Records.newRecord(classOf[LocalResource])
      amJarRsrc.setType(LocalResourceType.FILE)
      amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION)
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
      amJarRsrc.setTimestamp(jarStatus.getModificationTime)
      amJarRsrc.setSize(jarStatus.getLen)
      (jarPath.getName -> amJarRsrc)
    }.toMap
    amContainer.setLocalResources(localResources)

    // Set up the environment needed for the launch context
    val environment = Map(Environment.CLASSPATH.name ->
      List(Environment.CLASSPATH.$,
        "./*",
        Environment.HADOOP_CONF_DIR.$,
        Environment.HADOOP_COMMON_HOME.$ + "/*",
        Environment.HADOOP_COMMON_HOME.$ + "/lib/*",
        Environment.HADOOP_HDFS_HOME.$ + "/*",
        Environment.HADOOP_HDFS_HOME.$ + "/lib/*",
        Environment.YARN_HOME.$ + "/*",
        Environment.YARN_HOME.$ + "/lib/*").mkString(System.getProperty("path.separator")))
    amContainer.setEnvironment(environment)
    LOG.info("ApplicationManager environment: " + environment)

    // Construct the command to be executed on the launched container
    val command = List(
      Environment.JAVA_HOME.$ + "/bin/java",
      "st.happy_camper.hadoop.yarn.applications.simple.SimpleYarnApplicationManager",
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT,
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR)
    amContainer.setCommands(command)
    LOG.info("ApplicationManager Command: " + command.mkString(" "))

    // Define the resource requirements for the container
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(1024)
    amContainer.setResource(capability)

    // Set the container launch content into the ApplicationSubmittionContext
    appContext.setAMContainerSpec(amContainer)

    // Create the request to send to the ApplicationsManager
    val appRequest = Records.newRecord(classOf[SubmitApplicationRequest])
    appRequest.setApplicationSubmissionContext(appContext)
    applicationsManager.submitApplication(appRequest)

    // Monitor the Application
    @tailrec
    def monitor(appId: ApplicationId): Boolean = {
      val reportRequest = Records.newRecord(classOf[GetApplicationReportRequest])
      reportRequest.setApplicationId(appId)
      val reportResponse = applicationsManager.getApplicationReport(reportRequest)
      val report = reportResponse.getApplicationReport

      val state = report.getYarnApplicationState
      state match {
        case FINISHED =>
          report.getFinalApplicationStatus == FinalApplicationStatus.SUCCEEDED
        case KILLED => false
        case FAILED => false
        case _      => monitor(appId)
      }
    }

    if (monitor(application.getApplicationId)) 0 else 1
  }
}

/**
 * @author ueshin
 *
 */
object SimpleYarnApplicationClient {

  val LOG = LogFactory.getLog(classOf[SimpleYarnApplicationClient])

  val ApplicationName = "SimpleYarnApplication"

  /**
   * @param args
   */
  def main(args: Array[String]) {
    sys.exit(ToolRunner.run(new SimpleYarnApplicationClient, args))
  }

}
