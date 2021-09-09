/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Factory of remote tasks.
 *
 * <p>
 *
 * <ul>
 *   <li>V2 (11/09/2017)
 * </ul>
 *
 * @author Luigi Dell'Aquila (l.dellaquila--at--orientdb.com)
 */
public class ODefaultRemoteTaskFactoryV3 implements ORemoteTaskFactory {
  @Override
  public ORemoteTask createTask(final int code) {
    switch (code) {
      case OSQLCommandTask.FACTORYID: // 5
        return new OSQLCommandTask();

      case OScriptTask.FACTORYID: // 6
        return new OScriptTask();

      case OStopServerTask.FACTORYID: // 9
        return new OStopServerTask();

      case ORestartServerTask.FACTORYID: // 10
        return new ORestartServerTask();

      case OSyncClusterTask.FACTORYID: // 12
        return new OSyncClusterTask();

      case OSyncDatabaseTask.FACTORYID: // 14
        return new OSyncDatabaseTask();

      case OCopyDatabaseChunkTask.FACTORYID: // 15
        return new OCopyDatabaseChunkTask();

      case OGossipTask.FACTORYID: // 16
        return new OGossipTask();

      case ODropDatabaseTask.FACTORYID: // 23
        return new ODropDatabaseTask();

      case OUpdateDatabaseConfigurationTask.FACTORYID: // 24
        return new OUpdateDatabaseConfigurationTask();

      case ORequestDatabaseConfigurationTask.FACTORYID: // 27
        return new ORequestDatabaseConfigurationTask();

      case OUnreachableServerLocalTask.FACTORYID: // 28
        throw new IllegalArgumentException(
            "Task with code " + code + " is not supported in remote configuration");

      case OEnterpriseStatsTask.FACTORYID: // 29
        return new OEnterpriseStatsTask();

        // --- here starts V2 ----

      case ORunQueryExecutionPlanTask.FACTORYID: // 40
        return new ORunQueryExecutionPlanTask();

      case OFetchQueryPageTask.FACTORYID: // 41
        return new OFetchQueryPageTask();

      case OCloseQueryTask.FACTORYID: // 42
        return new OCloseQueryTask();

      case OTransactionPhase1Task.FACTORYID: // 43
        return new OTransactionPhase1Task();

      case OTransactionPhase2Task.FACTORYID: // 44
        return new OTransactionPhase2Task();

      case NodeOperationTask.FACTORYID: // 55
        return new NodeOperationTask();

      case ONewSQLCommandTask.FACTORYID: // 56
        return new ONewSQLCommandTask();

      case OSyncDatabaseNewDeltaTask.FACTORYID: // 57
        return new OSyncDatabaseNewDeltaTask();

      case OUpdateDatabaseSequenceStatusTask.FACTORYID: // 58
        return new OUpdateDatabaseSequenceStatusTask();
    }

    throw new IllegalArgumentException("Task with code " + code + " is not supported");
  }

  @Override
  public int getProtocolVersion() {
    return 3;
  }
}
