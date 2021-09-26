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
 */
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.*;
import java.util.*;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class BucketIterator implements Iterator<Record> {

  private final DatabaseInternal database;
  private final Bucket           bucket;
  int      nextPageNumber      = 0;
  BasePage currentPage         = null;
  short    recordCountInCurrentPage;
  int      totalPages;
  Record   next                = null;
  int      currentRecordInPage = 0;

  BucketIterator(Bucket bucket, Database db) {
    ((DatabaseInternal) db).checkPermissionsOnFile(bucket.id, SecurityDatabaseUser.ACCESS.READ_RECORD);

    this.database = (DatabaseInternal) db;
    this.bucket = bucket;
    this.totalPages = bucket.pageCount.get();

    final Integer txPageCounter = database.getTransaction().getPageCounter(bucket.id);
    if (txPageCounter != null && txPageCounter > totalPages)
      this.totalPages = txPageCounter;

    fetchNext();
  }

  public void setPosition(final RID position) throws IOException {
    next = position.getRecord();
    nextPageNumber = (int) (position.getPosition() / bucket.getMaxRecordsInPage());
    currentRecordInPage = (int) (position.getPosition() % bucket.getMaxRecordsInPage()) + 1;
    currentPage = database.getTransaction().getPage(new PageId(position.getBucketId(), nextPageNumber), bucket.pageSize);
    recordCountInCurrentPage = currentPage.readShort(Bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
  }

  private void fetchNext() {
    database.executeInReadLock(() -> {
      next = null;
      while (true) {
        if (currentPage == null) {
          if (nextPageNumber > totalPages) {
            return null;
          }
          currentPage = database.getTransaction().getPage(new PageId(bucket.file.getFileId(), nextPageNumber), bucket.pageSize);
          recordCountInCurrentPage = currentPage.readShort(Bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        }

        if (recordCountInCurrentPage > 0 && currentRecordInPage < recordCountInCurrentPage) {
          int recordPositionInPage = (int) currentPage.readUnsignedInt(Bucket.PAGE_RECORD_TABLE_OFFSET + currentRecordInPage * INT_SERIALIZED_SIZE);
          final long[] recordSize = currentPage.readNumberAndSize(recordPositionInPage);
          if (recordSize[0] > 0) {
            // NOT DELETED
            final RID rid = new RID(database, bucket.id, ((long) nextPageNumber) * bucket.getMaxRecordsInPage() + currentRecordInPage);

            currentRecordInPage++;

            if (!bucket.existsRecord(rid))
              continue;

            next = rid.getRecord(false);
            return null;

          } else if (recordSize[0] == -1) {
            // PLACEHOLDER
            final RID rid = new RID(database, bucket.id, ((long) nextPageNumber) * bucket.getMaxRecordsInPage() + currentRecordInPage);

            currentRecordInPage++;

            final Binary view = bucket.getRecordInternal(new RID(database, bucket.id, currentPage.readLong((int) (recordPositionInPage + recordSize[1]))),
                true);

            if (view == null)
              continue;

            next = database.getRecordFactory()
                .newImmutableRecord(database, database.getSchema().getType(database.getSchema().getTypeNameByBucketId(rid.getBucketId())), rid, view, null);
            return null;
          }

          currentRecordInPage++;

        } else if (currentRecordInPage == recordCountInCurrentPage) {
          currentRecordInPage = 0;
          currentPage = null;
          nextPageNumber++;
        } else {
          currentRecordInPage++;
        }
      }
    });
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Record next() {
    if (next == null) {
      throw new IllegalStateException();
    }
    try {
      return next;
    } finally {
      try {
        fetchNext();
      } catch (Exception e) {
        throw new DatabaseOperationException("Cannot scan bucket '" + bucket.name + "'", e);
      }
    }
  }
}
