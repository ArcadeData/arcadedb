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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.logging.*;

public class FileContentResponse extends HAAbstractCommand {
  private String databaseName;
  private int    fileId;
  private String fileName;
  private int    pageFromInclusive;

  private Binary  pagesContent;
  private int     totalPages;
  private boolean last;

  public FileContentResponse() {
  }

  public FileContentResponse(final String databaseName, final int fileId, final String fileName, final int pageFromInclusive,
      final Binary pagesContent, final int totalPages, final boolean last) {
    this.databaseName = databaseName;
    this.fileId = fileId;
    this.fileName = fileName;
    this.pageFromInclusive = pageFromInclusive;

    this.pagesContent = pagesContent;
    this.totalPages = totalPages;
    this.last = last;
  }

  public Binary getPagesContent() {
    return pagesContent;
  }

  public int getPages() {
    return totalPages;
  }

  public boolean isLast() {
    return last;
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    final DatabaseInternal database = server.getServer().getDatabase(databaseName);
    final PageManager pageManager = database.getPageManager();

    try {
      final ComponentFile file = database.getFileManager()
          .getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

      if (totalPages == 0)
        return null;

      if (file instanceof PaginatedComponentFile pFile) {
        final int pageSize = pFile.getPageSize();

        if (pagesContent.size() != totalPages * pageSize) {
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on received chunk for file '%s': size=%s, expected=%s (totalPages=%d)",
                  file.getFileName(), FileUtils.getSizeAsString(pagesContent.size()),
                  FileUtils.getSizeAsString((long) totalPages * pageSize), totalPages);
          throw new ReplicationException("Invalid file chunk");
        }

        for (int i = 0; i < totalPages; ++i) {
          final PageId pageId = new PageId(database, file.getFileId(), pageFromInclusive + i);

          final MutablePage page = new MutablePage(pageId, pageSize);
          System.arraycopy(pagesContent.getContent(), i * pageSize, page.getTrackable().getContent(), 0, pageSize);
          page.loadMetadata();
          pageManager.overwritePage(page);

          LogManager.instance().log(this, Level.FINE, "Overwritten page %s v%d from the leader", null,//
              pageId, page.getVersion());
        }

        final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileByIdIfExists(file.getFileId());
        if (component != null) {
          final int lastPageNumber = pageFromInclusive + totalPages;
          component.updatePageCount(lastPageNumber);

          if (component instanceof LocalBucket bucket)
            // RESET CACHED RECORD COUNT
            bucket.setCachedRecordCount(-1);
        }
      } else
        LogManager.instance().log(this, Level.SEVERE, "Cannot write not paginated file %s from the leader", fileName);

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on installing file content from leader server", e);
      throw new ReplicationException("Error on installing file content from leader server", e);
    }

    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
    stream.putInt(fileId);
    stream.putString(fileName);
    stream.putInt(pageFromInclusive);
    stream.putUnsignedNumber(totalPages);
    stream.putBytes(pagesContent.getContent(), pagesContent.size());
    stream.putByte((byte) (last ? 1 : 0));
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    databaseName = stream.getString();
    fileId = stream.getInt();
    fileName = stream.getString();
    pageFromInclusive = stream.getInt();
    totalPages = (int) stream.getUnsignedNumber();
    pagesContent = new Binary(stream.getBytes());
    last = stream.getByte() == 1;
  }

  @Override
  public String toString() {
    return "file=" + totalPages + " pages (" + pagesContent.size() + " bytes)";
  }
}
