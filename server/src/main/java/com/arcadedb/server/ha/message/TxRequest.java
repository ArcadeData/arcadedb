/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.nio.channels.ClosedChannelException;
import java.util.logging.Level;

/**
 * Replicate a transaction. No response is expected.
 */
public class TxRequest extends TxRequestAbstract {
  private boolean                        waitForResponse;
  public  DatabaseChangeStructureRequest changeStructure;

  public TxRequest() {
  }

  public TxRequest(final String dbName, final Binary bufferChanges, final boolean waitForResponse) {
    super(dbName, bufferChanges);
    this.waitForResponse = waitForResponse;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte((byte) (waitForResponse ? 1 : 0));

    if (changeStructure != null) {
      stream.putByte((byte) 1);
      changeStructure.toStream(stream);
    } else
      stream.putByte((byte) 0);

    super.toStream(stream);
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    waitForResponse = stream.getByte() == 1;
    if (stream.getByte() == 1) {
      changeStructure = new DatabaseChangeStructureRequest();
      changeStructure.fromStream(server, stream);
    }
    super.fromStream(server, stream);
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    if (changeStructure != null)
      try {
        // APPLY CHANGE OF STRUCTURE FIRST
        changeStructure.updateFiles(db);

        // RELOAD THE SCHEMA BUT NOT INITIALIZE THE COMPONENTS (SOME NEW PAGES COULD BE IN THE TX ITSELF)
        db.getSchema().getEmbedded().load(PaginatedFile.MODE.READ_WRITE, false);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on changing database structure request from the leader node", e);
        throw new ReplicationException("Error on changing database structure request from the leader node", e);
      }

    final WALFile.WALTransaction walTx = readTxFromBuffer();

    try {
      LogManager.instance().log(this, Level.FINE, "Applying tx %d from server %s (modifiedPages=%d)...", walTx.txId, remoteServerName, walTx.pages.length);

      db.getTransactionManager().applyChanges(walTx);

    } catch (WALException e) {
      if (e.getCause() instanceof ClosedChannelException) {
        // CLOSE THE ENTIRE DB
        LogManager.instance().log(this, Level.SEVERE, "Closed file during transaction, closing the entire database (error=%s)", e.toString());
        db.getEmbedded().close();
      }
      throw e;
    }

    if (changeStructure != null)
      // INITIALIZE THE COMPONENTS (SOME NEW PAGES COULD BE IN THE TX ITSELF)
      db.getSchema().getEmbedded().initComponents();

    if (waitForResponse)
      return new OkResponse();

    return null;
  }

  @Override
  public String toString() {
    return "tx(" + databaseName + ")";
  }
}
