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

import org.apache.hadoop.conf._
import org.apache.hadoop.net._
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf._
import org.apache.hadoop.yarn.ipc._
import org.apache.hadoop.yarn.util._

/**
 * @author ueshin
 *
 */
object SimpleYarnApplicationManager {

  /**
   * @param args
   */
  def main(args: Array[String]) {

    val conf = new Configuration
    val rpc = YarnRPC.create(conf)

    // Get containerId
    val containerId = ConverterUtils.toContainerId(
      sys.env.getOrElse(ApplicationConstants.AM_CONTAINER_ID_ENV,
        throw new IllegalArgumentException("ContainerId not set in the environment")))
    val appAttemptId = containerId.getApplicationAttemptId

    // Connect to the Scheduler of the ResourceManager
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    val resourceManager = rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]

    // Register the AM with the RM
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    //    appMasterRequest.setHost(appMasterHostname)
    //    appMasterRequest.setRpcPort(appMasterRpcPort)
    //    appMasterRequest.setTrackingUrl(appMasterTrackingUrl)
    val response = resourceManager.registerApplicationMaster(appMasterRequest)

    // TODO: Do something on containers

    // Finish
    val finishRequest = Records.newRecord(classOf[FinishApplicationMasterRequest])
    finishRequest.setAppAttemptId(appAttemptId)
    finishRequest.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED)
    resourceManager.finishApplicationMaster(finishRequest)
  }

}
