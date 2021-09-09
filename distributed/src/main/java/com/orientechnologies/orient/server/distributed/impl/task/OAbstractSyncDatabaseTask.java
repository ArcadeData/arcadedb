/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import java.util.UUID;

/**
 * Abstract task for synchronization of database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public abstract class OAbstractSyncDatabaseTask extends OAbstractReplicatedTask
    implements OCommandOutputListener {
  public static final int CHUNK_MAX_SIZE = 8388608; // 8MB
  public static final String DEPLOYDB = "deploydb.";
  public static final int FACTORYID = 14;

  protected long random;

  public OAbstractSyncDatabaseTask() {
    random = UUID.randomUUID().getLeastSignificantBits();
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n")) iText = iText.substring(2);
    if (iText.startsWith("\n")) iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return false;
  }
}
