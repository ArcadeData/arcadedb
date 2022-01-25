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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Forward a command to the Leader server to be executed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CommandForwardRequest extends HAAbstractCommand {
  private DatabaseInternal              database;
  private String                        databaseName;
  private String                        language;
  private String                        command;
  private LinkedHashMap<String, Object> namedParameters;
  private Object[]                      ordinalParameters;

  public CommandForwardRequest() {
  }

  public CommandForwardRequest(final DatabaseInternal database, final String language, final String command, final Map<String, Object> namedParameters,
      final Object[] ordinalParameters) {
    this.database = database;
    this.databaseName = database.getName();
    this.language = language;
    this.command = command;
    if (namedParameters != null) {
      this.namedParameters = new LinkedHashMap<>();
      this.namedParameters.putAll(namedParameters);
    }
    this.ordinalParameters = ordinalParameters;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
    stream.putString(language);
    stream.putString(command);

    if (namedParameters == null)
      stream.putInt(0);
    else {
      stream.putInt(namedParameters.size());
      for (Map.Entry<String, Object> entry : namedParameters.entrySet()) {
        stream.putString(entry.getKey());

        final byte type = Type.getTypeByValue(entry.getValue()).getBinaryType();
        stream.putByte(type);
        database.getSerializer().serializeValue(database, stream, type, entry.getValue());
      }
    }

    if (ordinalParameters == null)
      stream.putInt(0);
    else {
      stream.putInt(ordinalParameters.length);
      for (Object entry : ordinalParameters) {
        final byte type = Type.getTypeByValue(entry).getBinaryType();
        stream.putByte(type);
        database.getSerializer().serializeValue(database, stream, type, entry);
      }
    }
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    databaseName = stream.getString();
    language = stream.getString();
    command = stream.getString();

    database = (DatabaseInternal) server.getDatabase(databaseName);

    final int namedParametersSize = stream.getInt();
    if (namedParametersSize > 0) {
      namedParameters = new LinkedHashMap<>();
      for (int i = 0; i < namedParametersSize; i++) {
        final String key = stream.getString();
        final byte type = stream.getByte();
        final Object value = database.getSerializer().deserializeValue(database, stream, type, null);
        namedParameters.put(key, value);
      }
    }

    final int ordinalParametersSize = stream.getInt();
    if (ordinalParametersSize > 0) {
      ordinalParameters = new Object[ordinalParametersSize];
      for (int i = 0; i < ordinalParametersSize; i++) {
        final byte type = stream.getByte();
        ordinalParameters[i] = database.getSerializer().deserializeValue(database, stream, type, null);
      }
    }
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    final ResultSet result;
    if (namedParameters != null)
      result = db.command(language, command, namedParameters);
    else
      result = db.command(language, command, ordinalParameters);

    return new CommandForwardResponse(database, result);
  }

  @Override
  public String toString() {
    return "command-forward-request(" + databaseName + "," + language + "," + command + ")";
  }
}
