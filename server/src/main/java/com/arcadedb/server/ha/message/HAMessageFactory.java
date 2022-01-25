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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.ReplicationMessage;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class HAMessageFactory {
  private final ArcadeDBServer                        server;
  private final List<Class<? extends HACommand>>      commands   = new ArrayList<>();
  private final Map<Class<? extends HACommand>, Byte> commandMap = new HashMap<>();

  public HAMessageFactory(final ArcadeDBServer server) {
    this.server = server;

    registerCommand(ReplicaConnectRequest.class);
    registerCommand(ReplicaConnectFullResyncResponse.class);
    registerCommand(ReplicaConnectHotResyncResponse.class);
    registerCommand(DatabaseStructureRequest.class);
    registerCommand(DatabaseStructureResponse.class);
    registerCommand(DatabaseChangeStructureRequest.class);
    registerCommand(DatabaseChangeStructureResponse.class);
    registerCommand(FileContentRequest.class);
    registerCommand(FileContentResponse.class);
    registerCommand(DatabaseAlignRequest.class);
    registerCommand(DatabaseAlignResponse.class);
    registerCommand(TxRequest.class);
    registerCommand(OkResponse.class);
    registerCommand(TxForwardRequest.class);
    registerCommand(TxForwardResponse.class);
    registerCommand(CommandForwardRequest.class);
    registerCommand(CommandForwardResponse.class);
    registerCommand(ReplicaReadyRequest.class);
    registerCommand(UpdateClusterConfiguration.class);
    registerCommand(ErrorResponse.class);
  }

  public void serializeCommand(final HACommand command, final Binary buffer, final long messageNumber) {
    buffer.clear();
    buffer.putByte(getCommandId(command));
    buffer.putLong(messageNumber);
    command.toStream(buffer);
    buffer.flip();
  }

  public Pair<ReplicationMessage, HACommand> deserializeCommand(final Binary buffer, final byte[] requestBytes) {
    buffer.clear();
    buffer.putByteArray(requestBytes);
    buffer.flip();

    final byte commandId = buffer.getByte();

    final HACommand request = createCommandInstance(commandId);

    if (request != null) {
      final long messageNumber = buffer.getLong();
      request.fromStream(server, buffer);

      buffer.rewind();
      return new Pair<>(new ReplicationMessage(messageNumber, buffer), request);
    }

    LogManager.instance().log(this, Level.SEVERE, "Error on reading request, command %d not valid", commandId);
    return null;
  }

  private void registerCommand(final Class<? extends HACommand> commandClass) {
    commands.add(commandClass);
    commandMap.put(commandClass, (byte) (commands.size() - 1));
  }

  private HACommand createCommandInstance(final byte type) {
    if (type > commands.size())
      throw new IllegalArgumentException("Command with id " + type + " was not found");

    try {
      return commands.get(type).newInstance();
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on creating replication command", e);
      throw new ConfigurationException("Error on creating replication command", e);
    }
  }

  private byte getCommandId(final HACommand command) {
    final Byte commandId = commandMap.get(command.getClass());
    if (commandId == null)
      throw new IllegalArgumentException("Command of class " + command.getClass() + " was not found");

    return commandId;
  }
}
