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
import com.arcadedb.server.ha.ReplicationLogFile;
import com.arcadedb.server.ha.ReplicationMessage;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Request from a replica to the leader to resync missing log entries.
 * This is sent when a replica detects a gap in the message sequence instead of
 * performing a full reconnect, which is more efficient for small gaps.
 *
 * <p>The replica specifies the range of message numbers it needs:
 * [fromMessageNumber, toMessageNumber] inclusive.</p>
 */
public class ResyncRequest extends HAAbstractCommand {

  private long fromMessageNumber;
  private long toMessageNumber;

  public ResyncRequest() {
  }

  /**
   * Creates a resync request for a range of message numbers.
   *
   * @param fromMessageNumber The first message number needed (inclusive)
   * @param toMessageNumber   The last message number needed (inclusive)
   */
  public ResyncRequest(final long fromMessageNumber, final long toMessageNumber) {
    this.fromMessageNumber = fromMessageNumber;
    this.toMessageNumber = toMessageNumber;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(fromMessageNumber);
    stream.putLong(toMessageNumber);
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    fromMessageNumber = stream.getLong();
    toMessageNumber = stream.getLong();
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    LogManager.instance().log(this, Level.INFO,
        "Received resync request from replica '%s' for messages %d-%d",
        remoteServerName, fromMessageNumber, toMessageNumber);

    final ReplicationLogFile logFile = server.getReplicationLogFile();
    final List<ReplicationMessage> messages = new ArrayList<>();

    final long lastAvailable = logFile.getLastMessageNumber();

    // Check if the first requested message exists
    final long firstPosition = logFile.findMessagePosition(fromMessageNumber);
    if (firstPosition < 0) {
      // Requested messages have been truncated from the log or don't exist
      LogManager.instance().log(this, Level.WARNING,
          "Replica '%s' requested message %d but it's not available - full resync needed",
          remoteServerName, fromMessageNumber);
      return new ResyncResponse(fromMessageNumber, toMessageNumber, null, false,
          "Requested messages no longer available in log");
    }

    if (fromMessageNumber > lastAvailable) {
      // Replica is ahead of us? Should not happen
      LogManager.instance().log(this, Level.WARNING,
          "Replica '%s' requested message %d but latest is %d - replica is ahead?",
          remoteServerName, fromMessageNumber, lastAvailable);
      return new ResyncResponse(fromMessageNumber, toMessageNumber, null, false,
          "Requested messages are beyond current log");
    }

    // Clamp the range to what's available
    final long actualTo = Math.min(toMessageNumber, lastAvailable);

    // Fetch the messages from the log by iterating through positions
    long currentPosition = firstPosition;
    for (long msgNum = fromMessageNumber; msgNum <= actualTo; msgNum++) {
      try {
        final Pair<ReplicationMessage, Long> result = logFile.getMessage(currentPosition);
        if (result != null && result.getFirst() != null) {
          messages.add(result.getFirst());
          currentPosition = result.getSecond(); // Move to next message position
        } else {
          LogManager.instance().log(this, Level.WARNING,
              "Could not find message %d in log for replica '%s'", msgNum, remoteServerName);
          break; // Stop if we hit a gap
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error reading message %d from log for replica '%s': %s", msgNum, remoteServerName, e.getMessage());
        break;
      }
    }

    LogManager.instance().log(this, Level.INFO,
        "Sending %d messages (%d-%d) to replica '%s' for resync",
        messages.size(), fromMessageNumber, actualTo, remoteServerName);

    return new ResyncResponse(fromMessageNumber, actualTo, messages, true, null);
  }

  public long getFromMessageNumber() {
    return fromMessageNumber;
  }

  public long getToMessageNumber() {
    return toMessageNumber;
  }

  @Override
  public String toString() {
    return "ResyncRequest{from=" + fromMessageNumber + ", to=" + toMessageNumber + "}";
  }
}
