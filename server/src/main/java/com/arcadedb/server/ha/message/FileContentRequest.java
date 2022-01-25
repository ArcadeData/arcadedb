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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.io.IOException;

public class FileContentRequest extends HAAbstractCommand {
  private              String databaseName;
  private              int    fileId;
  private              int    fromPageInclusive; //  INCLUSIVE
  private              int    toPageInclusive; //  INCLUSIVE
  private static final int    CHUNK_MAX_PAGES = 10;

  public FileContentRequest() {
  }

  public FileContentRequest(final String dbName, final int fileId, final int pageFromInclusive, final int pageToInclusive) {
    this.databaseName = dbName;
    this.fileId = fileId;
    this.fromPageInclusive = pageFromInclusive;
    this.toPageInclusive = pageToInclusive;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    final PaginatedFile file = db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    try {
      final int totalPages = (int) (file.getSize() / pageSize);

      final Binary pagesContent = new Binary();

      int pages = 0;

      if (toPageInclusive == -1)
        toPageInclusive = totalPages - 1;

      for (int i = fromPageInclusive; i <= toPageInclusive && pages < CHUNK_MAX_PAGES; ++i) {
        final PageId pageId = new PageId(fileId, i);
        final BasePage page = db.getPageManager().getPage(pageId, pageSize, false, false).createImmutableView();
        pagesContent.putByteArray(page.getContent().array(), pageSize);

        ++pages;
      }

      final boolean last = pages > toPageInclusive;

      pagesContent.flip();

      return new FileContentResponse(databaseName, fileId, file.getFileName(), fromPageInclusive, pagesContent, pages, last);

    } catch (IOException e) {
      throw new NetworkProtocolException("Cannot load pages", e);
    }
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
    stream.putInt(fileId);
    stream.putInt(fromPageInclusive);
    stream.putInt(toPageInclusive);
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    databaseName = stream.getString();
    fileId = stream.getInt();
    fromPageInclusive = stream.getInt();
    toPageInclusive = stream.getInt();
  }

  @Override
  public String toString() {
    return "file(" + databaseName + " fileId=" + fileId + " fromPageInclusive=" + fromPageInclusive + " fromPageInclusive" + toPageInclusive + ")";
  }
}
