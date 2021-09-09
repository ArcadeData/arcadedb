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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OSyncSource;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Ask for synchronization of database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OSyncDatabaseTask extends OAbstractSyncDatabaseTask {
  public static final int FACTORYID = 14;

  public OSyncDatabaseTask() {}

  @Override
  public Object execute(
      final ODistributedRequestId requestId,
      final OServer iServer,
      final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {

    if (!iManager.getLocalNodeName().equals(getNodeSource())) {
      if (database == null) throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();
      final ODistributedDatabaseImpl dDatabase =
          (ODistributedDatabaseImpl) iManager.getMessageService().getDatabase(databaseName);

      try {
        final Long lastDeployment =
            (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
        if (lastDeployment != null && lastDeployment.longValue() == random) {
          // SKIP IT
          ODistributedServerLog.debug(
              this,
              iManager.getLocalNodeName(),
              getNodeSource(),
              DIRECTION.NONE,
              "Skip deploying database '%s' because already executed",
              databaseName);
          return Boolean.FALSE;
        }

        iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

        iManager.setDatabaseStatus(
            getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            DIRECTION.OUT,
            "Deploying database %s...",
            databaseName);

        OBackgroundBackup backup = null;
        OSyncSource last = dDatabase.getLastValidBackup();
        if (last instanceof OBackgroundBackup) {
          backup = (OBackgroundBackup) last;
        }

        if (backup == null || !last.isValid() || !backup.getResultedBackupFile().exists()) {
          // CREATE A BACKUP OF DATABASE FROM SCRATCH
          File backupFile =
              new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");
          String backupPath = backupFile.getAbsolutePath();

          final int compressionRate =
              OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger();

          if (backupFile.exists()) {
            if (backupFile.isDirectory()) {
              OFileUtils.deleteRecursively(backupFile);
            }
            backupFile.delete();
          } else backupFile.getParentFile().mkdirs();
          backupFile.createNewFile();

          final File resultedBackupFile = backupFile;

          final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
          if (completedFile.exists()) completedFile.delete();

          ODistributedServerLog.info(
              this,
              iManager.getLocalNodeName(),
              getNodeSource(),
              DIRECTION.OUT,
              "Creating backup of database '%s' (compressionRate=%d) in directory: %s...",
              databaseName,
              compressionRate,
              backupPath);

          backup =
              new OBackgroundBackup(
                  this,
                  iManager,
                  database,
                  resultedBackupFile,
                  backupPath,
                  null,
                  dDatabase,
                  requestId,
                  completedFile);
          Thread t = new Thread(backup);
          t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
          t.start();

          // RECORD LAST BACKUP TO BE REUSED IN CASE ANOTHER NODE ASK FOR THE SAME IN SHORT TIME
          // WHILE THE DB IS NOT UPDATED
          dDatabase.setLastValidBackup(backup);
        } else {
          backup.makeStreamFromFile();
          ODistributedServerLog.info(
              this,
              iManager.getLocalNodeName(),
              getNodeSource(),
              DIRECTION.OUT,
              "Reusing last backup of database '%s' in directory: %s...",
              databaseName,
              backup.getResultedBackupFile().getAbsolutePath());
        }

        while (!backup.getStarted().await(1, TimeUnit.MINUTES)) {
          OLogManager.instance()
              .info(
                  this,
                  "Another backup running on database '%s' waiting it to finish",
                  databaseName);
        }

        final ODistributedDatabaseChunk chunk =
            new ODistributedDatabaseChunk(backup, CHUNK_MAX_SIZE);

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            ODistributedServerLog.DIRECTION.OUT,
            "- transferring chunk #%d offset=%d size=%s...",
            1,
            0,
            OFileUtils.getSizeAsNumber(chunk.buffer.length));

        if (chunk.last) {
          // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
          iManager.setDatabaseStatus(
              iManager.getLocalNodeName(),
              databaseName,
              ODistributedServerManager.DB_STATUS.ONLINE);
        }

        return chunk;

      } catch (OLockException e) {
        ODistributedServerLog.debug(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            DIRECTION.NONE,
            "Skip deploying database %s because another node is doing it",
            databaseName);
      } finally {
        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            ODistributedServerLog.DIRECTION.OUT,
            "Deploy database task completed");
      }
    } else
      ODistributedServerLog.debug(
          this,
          iManager.getLocalNodeName(),
          getNodeSource(),
          DIRECTION.NONE,
          "Skip deploying database from the same node");

    return Boolean.FALSE;
  }

  @Override
  public String getName() {
    return "deploy_db";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeLong(random);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    random = in.readLong();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
