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
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseStructureRequest extends HAAbstractCommand {
  private String databaseName;

  public DatabaseStructureRequest() {
  }

  public DatabaseStructureRequest(final String dbName) {
    this.databaseName = dbName;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);

    final File file = new File(db.getDatabasePath() + "/" + EmbeddedSchema.SCHEMA_FILE_NAME);
    try {
      final String schemaJson;
      if (file.exists()) {
        try (FileInputStream fis = new FileInputStream(file)) {
          schemaJson = FileUtils.readStreamAsString(fis, db.getSchema().getEncoding());
        }
      } else
        schemaJson = "{}";

      final Map<Integer, String> fileNames = new HashMap<>();
      for (PaginatedFile f : db.getFileManager().getFiles())
        if (f != null)
          fileNames.put(f.getFileId(), f.getFileName());

      return new DatabaseStructureResponse(schemaJson, fileNames);

    } catch (IOException e) {
      throw new NetworkProtocolException("Error on reading schema json file", e);
    }
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    databaseName = stream.getString();
  }

  @Override
  public String toString() {
    return "dbstructure(" + databaseName + ")";
  }
}
