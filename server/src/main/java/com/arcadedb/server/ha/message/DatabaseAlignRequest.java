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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class DatabaseAlignRequest extends HAAbstractCommand {
  private String             databaseName;
  private String             schemaJson;
  private Map<Integer, Long> fileChecksums;
  private Map<Integer, Long> fileSizes;

  public DatabaseAlignRequest() {
  }

  public DatabaseAlignRequest(final String databaseName, final String schemaJson, final Map<Integer, Long> fileChecksums, final Map<Integer, Long> fileSizes) {
    this.databaseName = databaseName;
    this.schemaJson = schemaJson;
    this.fileChecksums = fileChecksums;
    this.fileSizes = fileSizes;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
    stream.putString(schemaJson);

    stream.putUnsignedNumber(fileChecksums.size());
    for (Map.Entry<Integer, Long> file : fileChecksums.entrySet()) {
      stream.putInt(file.getKey());
      stream.putLong(file.getValue());
    }

    stream.putUnsignedNumber(fileSizes.size());
    for (Map.Entry<Integer, Long> file : fileSizes.entrySet()) {
      stream.putInt(file.getKey());
      stream.putLong(file.getValue());
    }
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    databaseName = stream.getString();
    schemaJson = stream.getString();

    fileChecksums = new HashMap<>();
    int fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i) {
      final int fileId = stream.getInt();
      fileChecksums.put(fileId, stream.getLong());
    }

    fileSizes = new HashMap<>();
    fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i) {
      final int fileId = stream.getInt();
      fileSizes.put(fileId, stream.getLong());
    }
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal database = (DatabaseInternal) server.getServer().getDatabase(databaseName);

    final List<int[]> pagesToAlign = new ArrayList<>();

    // ACQUIRE A READ LOCK. TRANSACTION CAN STILL RUN, BUT CREATION OF NEW FILES (BUCKETS, TYPES, INDEXES) WILL BE PUT ON PAUSE UNTIL THIS LOCK IS RELEASED
    database.executeInReadLock(() -> {
      // AVOID FLUSHING OF DATA PAGES TO DISK
      database.getPageManager().suspendPageFlushing(true);

      try {
        for (Map.Entry<Integer, Long> entry : fileSizes.entrySet()) {
          final Integer fileId = entry.getKey();
          final PaginatedFile file = database.getFileManager().getFile(fileId);

          final Long leaderFileSize = entry.getValue();
          if (file.getSize() < leaderFileSize) {
            // ALIGN THE ENTIRE FILE
            pagesToAlign.add(new int[] { fileId, 0, -1 });

            LogManager.instance().log(this, Level.INFO, "File %d size %s <> leader %s: requesting the entire file from the leader", null,//
                fileId, file.getSize(), leaderFileSize);
            continue;
          }

          final Long leaderFileChecksum = fileChecksums.get(fileId);
          if (leaderFileChecksum == null)
            continue;

          final long localFileChecksum = file.calculateChecksum();
          if (localFileChecksum != leaderFileChecksum) {
            // ALIGN THE ENTIRE FILE
            pagesToAlign.add(new int[] { fileId, 0, -1 });

            LogManager.instance().log(this, Level.INFO, "File %d checksum %s <> leader %s: requesting the entire file from the leader", null,//
                fileId, localFileChecksum, leaderFileChecksum);
            continue;
          }
        }

        // ASK FOR FILES
        final Binary buffer = new Binary();
        for (int[] entry : pagesToAlign) {
          final FileContentRequest fileAlign = new FileContentRequest(databaseName, entry[0], entry[1], entry[2]);
          server.getLeader().sendCommandToLeader(buffer, fileAlign, -1);
        }

      } finally {
        database.getPageManager().suspendPageFlushing(false);
      }
      return null;
    });

    return new DatabaseAlignResponse(pagesToAlign);
  }

  @Override
  public String toString() {
    return "DatabaseAlignRequest{" + databaseName + " fileChecksum=" + fileChecksums + " fileSizes=" + fileSizes + "}";
  }
}
