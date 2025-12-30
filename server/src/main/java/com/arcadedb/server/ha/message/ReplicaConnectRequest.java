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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.*;

public class ReplicaConnectRequest extends HAAbstractCommand {
  private long lastReplicationMessageNumber = -1;

  public ReplicaConnectRequest() {
  }

  public ReplicaConnectRequest(final long lastReplicationMessageNumber) {
    this.lastReplicationMessageNumber = lastReplicationMessageNumber;
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    if (lastReplicationMessageNumber > -1) {
      // Check if the message exists in the leader's replication log
      // This is critical when a replica reconnects to a different leader than it was originally connected to
      final long position = server.getReplicationLogFile().findMessagePosition(lastReplicationMessageNumber);

      if (position >= 0) {
        // Message found in leader's log - hot resync is possible
        LogManager.instance().log(this, Level.INFO, "Hot backup with Replica server '%s' is possible (lastReplicationMessageNumber=%d)", remoteServerName,
            lastReplicationMessageNumber);
        return new ReplicaConnectHotResyncResponse(lastReplicationMessageNumber);
      } else {
        // Message not found in leader's log - this happens when replica reconnects to a different leader
        // The new leader doesn't have the messages from the previous leader in its log
        LogManager.instance().log(this, Level.WARNING,
            "Replica '%s' requested hot resync from message %d but message not found in leader's log - forcing full resync",
            remoteServerName, lastReplicationMessageNumber);
        // Fall through to full resync
      }
    }

    // IN ANY OTHER CASE EXECUTE FULL SYNC
    return new ReplicaConnectFullResyncResponse(server.getServer().getDatabaseNames());
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(lastReplicationMessageNumber);
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    lastReplicationMessageNumber = stream.getLong();
  }

  @Override
  public String toString() {
    return "connect(" + lastReplicationMessageNumber + ")";
  }
}
