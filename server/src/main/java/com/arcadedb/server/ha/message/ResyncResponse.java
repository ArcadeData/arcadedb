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
import com.arcadedb.server.ha.ReplicationMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Response from the leader containing the requested log entries for resync.
 * This is sent in response to a ResyncRequest.
 *
 * <p>If the requested messages are not available (e.g., they've been truncated),
 * the success flag will be false and errorMessage will contain the reason.
 * In this case, the replica should fall back to a full resync.</p>
 */
public class ResyncResponse extends HAAbstractCommand {

  private long                       fromMessageNumber;
  private long                       toMessageNumber;
  private List<ReplicationMessage>   messages;
  private boolean                    success;
  private String                     errorMessage;

  public ResyncResponse() {
  }

  /**
   * Creates a resync response.
   *
   * @param fromMessageNumber The first message number in the response
   * @param toMessageNumber   The last message number in the response
   * @param messages          The list of messages (can be null if not successful)
   * @param success           Whether the resync was successful
   * @param errorMessage      Error message if not successful
   */
  public ResyncResponse(final long fromMessageNumber, final long toMessageNumber,
                        final List<ReplicationMessage> messages, final boolean success,
                        final String errorMessage) {
    this.fromMessageNumber = fromMessageNumber;
    this.toMessageNumber = toMessageNumber;
    this.messages = messages;
    this.success = success;
    this.errorMessage = errorMessage;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(fromMessageNumber);
    stream.putLong(toMessageNumber);
    stream.putByte((byte) (success ? 1 : 0));

    if (success && messages != null) {
      stream.putInt(messages.size());
      for (final ReplicationMessage msg : messages) {
        stream.putLong(msg.messageNumber);
        final byte[] payload = msg.payload.toByteArray();
        stream.putInt(payload.length);
        stream.putByteArray(payload);
      }
    } else {
      stream.putInt(0);
      stream.putString(errorMessage != null ? errorMessage : "");
    }
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    fromMessageNumber = stream.getLong();
    toMessageNumber = stream.getLong();
    success = stream.getByte() == 1;

    final int messageCount = stream.getInt();
    if (success && messageCount > 0) {
      messages = new ArrayList<>(messageCount);
      for (int i = 0; i < messageCount; i++) {
        final long msgNum = stream.getLong();
        final int payloadLength = stream.getInt();
        final byte[] payload = stream.getBytes(payloadLength);
        messages.add(new ReplicationMessage(msgNum, new Binary(payload)));
      }
    } else if (!success) {
      errorMessage = stream.getString();
    }
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    // This is executed on the replica when receiving the response from the leader
    if (!success) {
      LogManager.instance().log(this, Level.WARNING,
          "Resync failed from leader '%s': %s - will need full resync",
          remoteServerName, errorMessage);
      return null;
    }

    LogManager.instance().log(this, Level.INFO,
        "Received %d messages (%d-%d) from leader '%s' for resync",
        messages != null ? messages.size() : 0, fromMessageNumber, toMessageNumber, remoteServerName);

    // Apply the messages to the local log
    if (messages != null) {
      for (final ReplicationMessage msg : messages) {
        if (!server.getReplicationLogFile().appendMessage(msg)) {
          LogManager.instance().log(this, Level.WARNING,
              "Failed to append message %d during resync", msg.messageNumber);
          // Continue with remaining messages
        }
      }
    }

    return null;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public List<ReplicationMessage> getMessages() {
    return messages;
  }

  public long getFromMessageNumber() {
    return fromMessageNumber;
  }

  public long getToMessageNumber() {
    return toMessageNumber;
  }

  @Override
  public String toString() {
    return "ResyncResponse{from=" + fromMessageNumber + ", to=" + toMessageNumber +
        ", success=" + success +
        ", messageCount=" + (messages != null ? messages.size() : 0) +
        (errorMessage != null ? ", error='" + errorMessage + "'" : "") +
        "}";
  }
}
