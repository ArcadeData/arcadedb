/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.nio.channels.ClosedChannelException;
import java.util.logging.Level;

/**
 * Replicate a transaction. No response is expected.
 */
public class TxRequest extends TxRequestAbstract {
  private boolean waitForResponse;

  public TxRequest() {
  }

  public TxRequest(final String dbName, final Binary bufferChanges, final boolean waitForResponse) {
    super(dbName, bufferChanges);
    this.waitForResponse = waitForResponse;
  }

  @Override
  public void toStream(Binary stream) {
    stream.putByte((byte) (waitForResponse ? 1 : 0));
    super.toStream(stream);
  }

  @Override
  public void fromStream(Binary stream) {
    waitForResponse = stream.getByte() == 1;
    super.fromStream(stream);
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    final WALFile.WALTransaction walTx = readTxFromBuffer();

    try {
      server.getServer().log(this, Level.FINE, "Applying tx %d from server %s (modifiedPages=%d)...", walTx.txId, remoteServerName, walTx.pages.length);

      db.getTransactionManager().applyChanges(walTx);

    } catch (WALException e) {
      if (e.getCause() instanceof ClosedChannelException) {
        // CLOSE THE ENTIRE DB
        server.getServer().log(this, Level.SEVERE, "Closed file during transaction, closing the entire database (error=%s)", e.toString());
        db.getEmbedded().close();
      }
      throw e;
    }

    if (waitForResponse)
      return new TxResponse();

    return null;
  }

  @Override
  public String toString() {
    return "tx(" + databaseName + ")";
  }
}
