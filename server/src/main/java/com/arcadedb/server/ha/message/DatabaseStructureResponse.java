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
import com.arcadedb.server.ha.HAServer;

import java.util.HashMap;
import java.util.Map;

public class DatabaseStructureResponse extends HAAbstractCommand {
  private String               schemaJson;
  private Map<Integer, String> fileNames;

  public DatabaseStructureResponse() {
  }

  public DatabaseStructureResponse(final String schemaJson, final Map<Integer, String> fileNames) {
    this.schemaJson = schemaJson;
    this.fileNames = fileNames;
  }

  public Map<Integer, String> getFileNames() {
    return fileNames;
  }

  public String getSchemaJson() {
    return schemaJson;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(schemaJson);

    stream.putUnsignedNumber(fileNames.size());
    for (Map.Entry<Integer, String> file : fileNames.entrySet()) {
      stream.putInt(file.getKey());
      stream.putByte((byte) (file.getValue() != null ? 1 : 0));
      if (file.getValue() != null)
        stream.putString(file.getValue());
    }
  }

  @Override
  public void fromStream(final Binary stream) {
    schemaJson = stream.getString();

    fileNames = new HashMap<>();
    final int fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i) {
      final int fileId = stream.getInt();
      final boolean notNull = stream.getByte() == 1;
      if (notNull)
        fileNames.put(fileId, stream.getString());
      else
        fileNames.put(fileId, null);
    }
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public String toString() {
    return "dbstructure=" + fileNames;
  }
}
