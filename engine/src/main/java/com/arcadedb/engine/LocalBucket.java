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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.LONG_SERIALIZED_SIZE;

/**
 * PAGE CONTENT = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(2048*uint=8192)]
 * <br><br>
 * Record size is the length of the record, managing also the following special cases:
 * <ul>
 * <li>> 0 = active record</li>
 * <li>0 = deleted record</li>
 * <li>-1 = placeholder pointer that points to another record in another page</li>
 * <li>-2 = first chunk of a multi page record</li>
 * <li>-3 = after first chunk of a multi page record</li>
 * <li><-5 = placeholder content pointed from another record in another page</li>
 * </ul>
 * The record size is stored as a varint (variable integer size) + the length of the size itself.
 * So if the record size is 10, it would take 1 byte (to store the number 10 in 7 bits) + 1 byte (to store how many bytes is 10))
 * The minimum size of a record stored in a page is 5 bytes. If the record is smaller than 5 bytes,
 * it is filled with blanks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalBucket extends PaginatedComponent implements Bucket {
  public static final    String                    BUCKET_EXT                       = "bucket";
  public static final    int                       CURRENT_VERSION                  = 0;
  public static final    long                      RECORD_PLACEHOLDER_POINTER       = -1L;    // USE -1 AS SIZE TO STORE A PLACEHOLDER (THAT POINTS TO A RECORD ON ANOTHER PAGE)
  public static final    long                      FIRST_CHUNK                      = -2L;    // USE -2 TO MARK THE FIRST CHUNK OF A BIG RECORD. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK
  public static final    long                      NEXT_CHUNK                       = -3L;    // USE -3 TO MARK THE SECOND AND FURTHER CHUNK THAT IS PART OF A BIG RECORD THAT DOES NOT FIT A PAGE. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK OR 0 IF THE CURRENT CHUNK IS THE LAST (NO FURTHER CHUNKS)
  protected static final int                       PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int                       PAGE_RECORD_TABLE_OFFSET         =
      PAGE_RECORD_COUNT_IN_PAGE_OFFSET + Binary.SHORT_SERIALIZED_SIZE;
  private static final   int                       DEF_MAX_RECORDS_IN_PAGE          = 2048;
  private static final   int                       MINIMUM_RECORD_SIZE              = 5;    // RECORD SIZE CANNOT BE < 13 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER AND 1ST CHUCK FOR MULTI-PAGE CONTENT
  private static final   long                      RECORD_PLACEHOLDER_CONTENT       =
      MINIMUM_RECORD_SIZE * -1L;    // < -5 FOR SURROGATE RECORDS
  private static final   long                      MINIMUM_SPACE_LEFT_IN_PAGE       = 50L;
  private static final   int                       MAX_PAGES_GATHER_STATS           = 100;
  private static final   long                      MAX_TIMEOUT_GATHER_STATS         = 5000L;
  private static final   int                       GATHER_STATS_MIN_SPACE_PERC      = 10;
  private static final   int                       SPARE_SPACE_FOR_GROWTH           = 32;
  protected final        int                       contentHeaderSize;
  private final          int                       maxRecordsInPage                 = DEF_MAX_RECORDS_IN_PAGE;
  private final          AtomicLong                cachedRecordCount                = new AtomicLong(-1);
  private final          TreeMap<Integer, Integer> freeSpaceInPages                 = new TreeMap<>();
  private final          REUSE_SPACE_MODE          reuseSpaceMode;
  private                long                      timeOfLastStats                  = 0L;
  private                long                      changesFromLastStats             = 0L;

  private enum REUSE_SPACE_MODE {
    LOW, MEDIUM, HIGH
  }

  private static class PageAnalysis {
    public final BasePage page;
    public       int      newRecordPositionInPage     = -1;
    public       int      availablePositionIndex      = -1;
    public       boolean  createNewPage               = false;
    public       int      totalRecordsInPage          = -1;
    public       int      spaceAvailableInCurrentPage = -1;
    public       int      lastRecordPositionInPage    = -1;

    public PageAnalysis(final BasePage page) {
      this.page = page;
    }
  }

  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LocalBucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * Called at creation time.
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    super(database, name, filePath, BUCKET_EXT, mode, pageSize, version);
    this.contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.cachedRecordCount.set(0);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
  }

  /**
   * Called at load time.
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
    if (this.reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.HIGH.ordinal())
      gatherPageStatistics();
  }

  @Override
  public void close() {
    super.close();
    freeSpaceInPages.clear();
  }

  public int getMaxRecordsInPage() {
    return maxRecordsInPage;
  }

  @Override
  public RID createRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    return createRecordInternal(record, false, discardRecordAfter);
  }

  @Override
  public void updateRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);
    updateRecordInternal(record, record.getIdentity(), false, discardRecordAfter);
  }

  @Override
  public Binary getRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Binary rec = getRecordInternal(rid, false);
    if (rec == null)
      // DELETED
      throw new RecordNotFoundException("Record " + rid + " not found", rid);
    return rec;
  }

  @Override
  public boolean existsRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        return false;
    }

    try {
      final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        return false;

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
        return false;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

      return recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  @Override
  public void deleteRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false);
  }

  @Override
  public void scan(final RawRecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final RID rid = new RID(database, fileId, ((long) pageId) * maxRecordsInPage + recordIdInPage);

            try {
              final int recordPositionInPage = getRecordPositionInPage(page, recordIdInPage);
              if (recordPositionInPage == 0)
                // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
                continue;

              final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

              if (recordSize[0] > 0) {
                // NOT DELETED
                final int recordContentPositionInPage = recordPositionInPage + (int) recordSize[1];

                final Binary view = page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

                if (!callback.onRecord(rid, view))
                  return;

              } else if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
                // LOAD PLACEHOLDER CONTENT
                final RID placeHolderPointer = new RID(database, fileId,
                    page.readLong((int) (recordPositionInPage + recordSize[1])));
                final Binary view = getRecordInternal(placeHolderPointer, true);
                if (view != null && !callback.onRecord(rid, view))
                  return;
              } else if (recordSize[0] == FIRST_CHUNK) {
                // LOAD THE ENTIRE RECORD IN CHUNKS
                final Binary view = loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
                if (!callback.onRecord(rid, view))
                  return;
              }

            } catch (final Exception e) {
              final boolean reThrowException = e instanceof DatabaseIsReadOnlyException;

              if (errorRecordCallback != null) {
                if (!errorRecordCallback.onErrorLoading(rid, e))
                  // STOP THE SCAN
                  return;
              } else if (!reThrowException)
                // LOG THE EXCEPTION
                LogManager.instance()
                    .log(this, Level.SEVERE, "Error on loading record %s (error: %s)".formatted(rid, e.getMessage()));

              if (reThrowException)
                throw e;
            }
          }
        }
      }
    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot scan bucket '" + componentName + "'", e);
    }
  }

  public void fetchPageInTransaction(final RID rid) throws IOException {
    if (rid.getPosition() < 0L) {
      LogManager.instance().log(this, Level.WARNING, "Cannot load a page from a record with invalid RID (" + rid + ")");
      return;
    }

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount) {
        LogManager.instance().log(this, Level.WARNING, "Record " + rid + " not found");
      }
    }

    database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
  }

  @Override
  public Iterator<Record> iterator() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);
    return new BucketIterator(this, true);
  }

  @Override
  public Iterator<Record> inverseIterator() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);
    return new BucketIterator(this, false);
  }

  @Override
  public String toString() {
    return componentName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof LocalBucket))
      return false;

    return ((LocalBucket) obj).fileId == this.fileId;
  }

  @Override
  public int hashCode() {
    return fileId;
  }

  @Override
  public long count() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final TransactionContext transaction = database.getTransactionIfExists();

    final long cached = cachedRecordCount.get();
    if (cached > -1 && transaction != null)
      return cached + transaction.getBucketRecordDelta(fileId);

    long total = 0;

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final PageId pageIdToLoad = new PageId(database, file.getFileId(), pageId);
        final BasePage page = transaction != null ?
            transaction.getPage(pageIdToLoad, pageSize) :
            database.getPageManager().getImmutablePage(pageIdToLoad, pageSize, false, false);

        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final int recordPositionInPage = getRecordPositionInPage(page, recordIdInPage);
            if (recordPositionInPage == 0)
              // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
              continue;

            final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

            if (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK)
              total++;
          }
        }
      }

      cachedRecordCount.set(total);

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot count bucket '" + componentName + "'", e);
    }
    return total;
  }

  public Map<String, Object> check(final int verboseLevel, final boolean fix) {
    final Map<String, Object> stats = new HashMap<>();

    final int totalPages = getTotalPages();

    if (verboseLevel > 1)
      LogManager.instance()
          .log(this, Level.INFO, "- Checking bucket '%s' (totalPages=%d spaceOnDisk=%s pageSize=%s)...", componentName, totalPages,
              FileUtils.getSizeAsString((long) totalPages * pageSize), FileUtils.getSizeAsString(pageSize));

    long totalAllocatedRecords = 0L;
    long totalActiveRecords = 0L;
    long totalPlaceholderRecords = 0L;
    long totalSurrogateRecords = 0L;
    long totalMultiPageRecords = 0L;
    long totalDeletedRecords = 0L;
    long totalMaxOffset = 0L;
    long totalChunks = 0L;

    long totalErrors = 0L;
    final List<String> warnings = new ArrayList<>();
    final List<RID> deletedRecordsAfterFix = new ArrayList<>();

    String warning = null;

    for (int pageId = 0; pageId < totalPages; ++pageId) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        int pageActiveRecords = 0;
        int pagePlaceholderRecords = 0;
        int pageSurrogateRecords = 0;
        int pageDeletedRecords = 0;
        int pageMaxOffset = 0;
        int pageMultiPageRecords = 0;
        int pageChunks = 0;

        for (int positionInPage = 0; positionInPage < recordCountInPage; ++positionInPage) {
          final RID rid = new RID(database, file.getFileId(), pageId * maxRecordsInPage + positionInPage);

          final int recordPositionInPage = (int) page.readUnsignedInt(
              PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);

          if (recordPositionInPage == 0) {
            // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
            pageDeletedRecords++;
            totalDeletedRecords++;

          } else if (recordPositionInPage > page.getContentSize()) {
            ++totalErrors;
            warning = "invalid record offset %d in page for record %s".formatted(recordPositionInPage, rid);
            if (fix) {
              deleteRecordInternal(rid, true, true);
              deletedRecordsAfterFix.add(rid);
              ++totalDeletedRecords;
            }
          } else {

            try {
              final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

              totalAllocatedRecords++;

              if (recordSize[0] == 0) {
                pageDeletedRecords++;
                totalDeletedRecords++;
              } else if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
                pagePlaceholderRecords++;
                totalPlaceholderRecords++;
                recordSize[0] = MINIMUM_RECORD_SIZE;
              } else if (recordSize[0] == FIRST_CHUNK) {
                pageActiveRecords++;
                pageMultiPageRecords++;
                totalMultiPageRecords++;
                recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] == NEXT_CHUNK) {
                totalChunks++;
                pageChunks++;
                recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
                pageSurrogateRecords++;
                totalSurrogateRecords++;
                recordSize[0] *= -1;
              } else {
                pageActiveRecords++;
                totalActiveRecords++;
              }

              final long endPosition = recordPositionInPage + recordSize[1] + recordSize[0];
              if (endPosition > file.getPageSize()) {
                ++totalErrors;
                warning = "wrong record size %d found for record %s".formatted(recordSize[1] + recordSize[0], rid);
                if (fix) {
                  deleteRecordInternal(rid, true, true);
                  deletedRecordsAfterFix.add(rid);
                  ++totalDeletedRecords;
                }
              }

              if (endPosition > pageMaxOffset)
                pageMaxOffset = (int) endPosition;

            } catch (final Exception e) {
              ++totalErrors;
              warning = "unknown error on loading record %s: %s".formatted(rid, e.getMessage());

              if (fix && !(e instanceof RecordNotFoundException)) {
                deleteRecordInternal(rid, true, true);
                deletedRecordsAfterFix.add(rid);
                ++totalDeletedRecords;
              }
            }
          }

          if (warning != null) {
            warnings.add(warning);
            if (verboseLevel > 0)
              LogManager.instance().log(this, Level.SEVERE, "- " + warning);
            warning = null;
          }
        }

        totalMaxOffset += pageMaxOffset;

        if (verboseLevel > 2)
          LogManager.instance().log(this, Level.FINE,
              "-- Page %d records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d multiPageRecords=%d chunks=%d) maxOffset=%d",
              pageId, recordCountInPage, pageActiveRecords, pageDeletedRecords, pagePlaceholderRecords, pageSurrogateRecords,
              pageMultiPageRecords, pageChunks, pageMaxOffset);

      } catch (final Exception e) {
        ++totalErrors;
        warning = "unknown error on checking page %d: %s".formatted(pageId, e.getMessage());
      }

      if (warning != null) {
        warnings.add(warning);
        if (verboseLevel > 0)
          LogManager.instance().log(this, Level.SEVERE, "- " + warning);
        warning = null;
      }
    }

    final float avgPageUsed = totalPages > 0 ? ((float) totalMaxOffset) / totalPages * 100F / pageSize : 0;

    if (verboseLevel > 1)
      LogManager.instance()
          .log(this, Level.INFO, "-- Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%",
              totalAllocatedRecords, totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords,
              avgPageUsed);

    stats.put("pageSize", (long) pageSize);
    stats.put("totalPages", (long) totalPages);
    stats.put("totalAllocatedRecords", totalAllocatedRecords);
    stats.put("totalActiveRecords", totalActiveRecords);
    stats.put("totalPlaceholderRecords", totalPlaceholderRecords);
    stats.put("totalSurrogateRecords", totalSurrogateRecords);
    stats.put("totalDeletedRecords", totalDeletedRecords);
    stats.put("totalMaxOffset", totalMaxOffset);
    stats.put("totalMultiPageRecords", totalMultiPageRecords);
    stats.put("totalChunks", totalChunks);

    final DocumentType type = database.getSchema().getTypeByBucketId(fileId);
    if (type instanceof LocalVertexType) {
      stats.put("totalAllocatedVertices", totalAllocatedRecords);
      stats.put("totalActiveVertices", totalActiveRecords);
    } else if (type instanceof LocalEdgeType) {
      stats.put("totalAllocatedEdges", totalAllocatedRecords);
      stats.put("totalActiveEdges", totalActiveRecords);
    } else {
      stats.put("totalAllocatedDocuments", totalAllocatedRecords);
      stats.put("totalActiveDocuments", totalActiveRecords);
    }

    stats.put("deletedRecordsAfterFix", deletedRecordsAfterFix);
    stats.put("warnings", warnings);
    stats.put("autoFix", 0L);
    stats.put("totalErrors", totalErrors);

    return stats;
  }

  /**
   * The caller should call @{@link DatabaseInternal#invokeAfterReadEvents(Record)} after created the record and manage the result correctly.
   */
  public Binary getRecordInternal(final RID rid, final boolean readPlaceHolderContent) {
    // INVOKE EVENT CALLBACKS
    if (!((RecordEventsRegistry) database.getEvents()).onBeforeRead(rid))
      return null;
    final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
    if (type != null)
      if (!((RecordEventsRegistry) type.getEvents()).onBeforeRead(rid))
        return null;

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
        return null;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] == 0)
        // DELETED
        return null;

      if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        if (!readPlaceHolderContent)
          // PLACEHOLDER
          return null;

        recordSize[0] *= -1;
      }

      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
        // FOUND PLACEHOLDER, LOAD THE REAL RECORD
        final RID placeHolderPointer = new RID(database, rid.getBucketId(),
            page.readLong((int) (recordPositionInPage + recordSize[1])));
        return getRecordInternal(placeHolderPointer, true);
      } else if (recordSize[0] == FIRST_CHUNK) {
        // FOUND 1ST CHUNK, LOAD THE ENTIRE MULTI-PAGE RECORD
        return loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
      } else if (recordSize[0] == NEXT_CHUNK)
        // CANNOT LOAD PARTIAL CHUNK
        return null;

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on lookup of record " + rid, e);
    }
  }

  public long getCachedRecordCount() {
    return cachedRecordCount.get();
  }

  public void setCachedRecordCount(final long count) {
    cachedRecordCount.set(count);
  }

  private RID createRecordInternal(final Record record, final boolean isPlaceHolder, final boolean discardRecordAfter) {
    final Binary buffer = database.getSerializer().serialize(database, record);

    // RECORD SIZE CANNOT BE < 13 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER. FILL THE DIFFERENCE WITH BLANK (0)
    while (buffer.size() < MINIMUM_RECORD_SIZE) {
      buffer.append((byte) 0);
    }

    final int bufferSize = buffer.size();

    try {
      int newRecordPositionInPage = -1;
      int availablePositionIndex = -1;
      boolean createNewPage = false;
      BasePage foundPage = null;

      final int txPageCounter = getTotalPages();

      final int spaceNeeded = Binary.getNumberSpace(isPlaceHolder ? (-1L * bufferSize) : bufferSize) + bufferSize;

      if (txPageCounter > 0) {
        final PageAnalysis pageAnalysis = findAvailableSpace(-1, spaceNeeded, txPageCounter, false);
        foundPage = pageAnalysis.page;
        newRecordPositionInPage = pageAnalysis.newRecordPositionInPage;
        availablePositionIndex = pageAnalysis.availablePositionIndex;
        createNewPage = pageAnalysis.createNewPage;
      } else
        createNewPage = true;

      final MutablePage selectedPage;
      if (createNewPage) {
        selectedPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), txPageCounter), pageSize);
        newRecordPositionInPage = contentHeaderSize;
        availablePositionIndex = 0;
      } else
        selectedPage = database.getTransaction().getPageToModify(foundPage);

      LogManager.instance()
          .log(this, Level.FINE, "Creating record (%s records=%d threadId=%d)", selectedPage, availablePositionIndex,
              Thread.currentThread().threadId());
      final RID rid = new RID(database, file.getFileId(),
          ((long) selectedPage.getPageId().getPageNumber()) * maxRecordsInPage + availablePositionIndex);

      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newRecordPositionInPage;

      // RESERVE A SPOT IMMEDIATELY TO AVOID USAGE FOR MULTI PAGE RECORD
      selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
          newRecordPositionInPage);

      short recordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (availablePositionIndex + 1 > recordCountInPage) {
        // UPDATE RECORD NUMBER
        recordCountInPage = (short) (availablePositionIndex + 1);
        selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, recordCountInPage);
      }

      if (spaceNeeded > spaceAvailableInCurrentPage) {
        // MULTI-PAGE RECORD
        writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage);

      } else {
        final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage, isPlaceHolder ? (-1L * bufferSize) : bufferSize);
        selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(),
            bufferSize);
        updatePageStatistics(selectedPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -spaceNeeded);
      }

      LogManager.instance()
          .log(this, Level.FINE, "Created record %s (%s records=%d threadId=%d)", rid, selectedPage, recordCountInPage,
              Thread.currentThread().threadId());

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + componentName + "'", e);
    }
  }

  private boolean updateRecordInternal(final Record record, final RID rid, final boolean updatePlaceholderContent,
      final boolean discardRecordAfter) {
    if (rid.getPosition() < 0)
      throw new IllegalArgumentException("Cannot update a record with invalid RID");

    final Binary buffer = database.getSerializer().serialize(database, record);

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final MutablePage page = database.getTransaction()
          .getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == 0L)
        // DELETED
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

//      LogManager.instance()
//          .log(this, Level.SEVERE, "UPDATE %s pageV=%d content %s (threadId=%d)", rid, page.getVersion(), record.toJSON(), Thread.currentThread().threadId());

      boolean isPlaceHolder = false;
      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {

        // FOUND A RECORD POINTED FROM A PLACEHOLDER
        final RID placeHolderContentRID = new RID(database, fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
        if (updateRecordInternal(record, placeHolderContentRID, true, discardRecordAfter)) {
          // UPDATE PLACEHOLDER CONTENT, THE PLACEHOLDER POINTER STAY THE SAME
          if (!discardRecordAfter)
            ((RecordInternal) record).setBuffer(buffer.getNotReusable());
          return true;
        }

        // DELETE OLD PLACEHOLDER, A NEW PLACEHOLDER WILL BE CREATED WITH ENOUGH SPACE
        deleteRecordInternal(placeHolderContentRID, true, false);

        recordSize[0] = LONG_SERIALIZED_SIZE;
        recordSize[1] = 1L;
      } else if (recordSize[0] == FIRST_CHUNK) {
        updateMultiPageRecord(rid, buffer, page, (int) (recordPositionInPage + recordSize[1]));

        if (!discardRecordAfter)
          ((RecordInternal) record).setBuffer(buffer.getNotReusable());

        return true;

      } else if (recordSize[0] == NEXT_CHUNK) {
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        if (!updatePlaceholderContent)
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        isPlaceHolder = true;
        recordSize[0] *= -1L;
      }

      final int bufferSize = buffer.size();
      if (bufferSize > recordSize[0]) {
        // UPDATED RECORD IS LARGER THAN THE PREVIOUS VERSION: MAKE ROOM IN THE PAGE IF POSSIBLE
        final int lastRecordPositionInPage = getLastRecordPositionInPage(page, recordCountInPage);

        final long[] lastRecordSize;
        if (lastRecordPositionInPage != recordPositionInPage) {
          // CURRENT RECORD IS NOT THE LATEST RIGHT IN THE PAGE

          if (lastRecordPositionInPage < contentHeaderSize)
            // IT SHOULD NEVER OCCUR BECAUSE THE CURRENT RECORD IS PRESENT IN THIS PAGE
            throw new DatabaseOperationException("Invalid position on expanding existing record " + rid);

          lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);

          if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER) {
            lastRecordSize[0] = LONG_SERIALIZED_SIZE;
            lastRecordSize[1] = 1L;
          } else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
            // CONSIDER THE CHUNK SIZE
            lastRecordSize[0] = page.readInt((int) (lastRecordPositionInPage + lastRecordSize[1]));
            lastRecordSize[1] = 1L + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
          } else if (lastRecordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
            lastRecordSize[0] *= -1L;
          }
        } else
          lastRecordSize = recordSize;

        final int pageOccupiedInBytes = (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]);
        final int spaceAvailableInCurrentPage = page.getMaxContentSize() - pageOccupiedInBytes;
        final int bufferSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int additionalSpaceNeeded = (int) (bufferSize + bufferSizeLength - recordSize[0] - recordSize[1]);

        if (additionalSpaceNeeded < spaceAvailableInCurrentPage) {
          // THERE IS SPACE LEFT IN THE PAGE, SHIFT ON THE RIGHT THE EXISTENT RECORDS

          if (lastRecordPositionInPage != recordPositionInPage) {
            // NOT LAST RECORD IN PAGE, SHIFT NEXT RECORDS
            final int from = (int) (recordPositionInPage + recordSize[0] + recordSize[1]);

            page.move(from, from + additionalSpaceNeeded, pageOccupiedInBytes - from);

            // TODO: CALCULATE THE REAL SIZE TO COMPACT DELETED RECORDS/PLACEHOLDERS
            for (int pos = 0; pos < recordCountInPage; ++pos) {
              final int nextRecordPosInPage = getRecordPositionInPage(page, pos);
              if (nextRecordPosInPage != 0 &&//
                  nextRecordPosInPage >= from &&//
                  nextRecordPosInPage <= pageOccupiedInBytes)
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE,
                    nextRecordPosInPage + additionalSpaceNeeded);

              assert nextRecordPosInPage + additionalSpaceNeeded < page.getMaxContentSize();
            }
          }

          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

          page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

          LogManager.instance()
              .log(this, Level.FINE, "Updated record %s by allocating new space on the same page (%s threadId=%d)", null, rid, page,
                  Thread.currentThread().threadId());

          updatePageStatistics(pageId, spaceAvailableInCurrentPage, -additionalSpaceNeeded);

        } else {
          if (isPlaceHolder)
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            return false;

          int availableSpaceInCurrentPage = (int) (recordSize[0] + recordSize[1]);
          if (lastRecordPositionInPage == recordPositionInPage)
            // SINCE IT'S THE LAST RECORD IN THE PAGE, GET ALSO THE REST OF THE SPACE AVAILABLE IN THE PAGE
            availableSpaceInCurrentPage += spaceAvailableInCurrentPage;

          // TODO: LOOK FOR 1/2 OF THE RECORD SIZE
          if (availableSpaceInCurrentPage < 2 + LONG_SERIALIZED_SIZE + INT_SERIALIZED_SIZE) {
            final int bytesWritten = page.writeNumber(recordPositionInPage, RECORD_PLACEHOLDER_POINTER);

            final RID realRID = createRecordInternal(record, true, false);
            page.writeLong(recordPositionInPage + bytesWritten, realRID.getPosition());

            LogManager.instance()
                .log(this, Level.FINE, "Updated record %s by allocating new space with a placeholder (%s threadId=%d)", null, rid,
                    page, Thread.currentThread().threadId());
          } else {
            // SPLIT THE RECORD IN CHUNKS AS LINKED LIST AND STORE THE FIRST PART ON CURRENT PAGE ISSUE https://github.com/ArcadeData/arcadedb/issues/332
            writeMultiPageRecord(rid, buffer, page, recordPositionInPage, availableSpaceInCurrentPage);

            LogManager.instance().log(this, Level.FINE,
                "Updated record %s by splitting it in multiple chunks to be saved in multiple pages (%s threadId=%d)", null, rid,
                page, Thread.currentThread().threadId());
          }
        }
      } else {
        // UPDATED RECORD CONTENT IS NOT LARGER THAN PREVIOUS VERSION: OVERWRITE THE CONTENT
        // CREATE A HOLE (REMOVED LATER BY COMPRESS-PAGE)
        recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
        page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

        LogManager.instance()
            .log(this, Level.FINE, "Updated record %s with the same size or less as before (%s threadId=%d)", null, rid, page,
                Thread.currentThread().threadId());
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return true;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on update record " + rid, e);
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholderContent, final boolean deleteChunks) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    database.getTransaction().removeRecordFromCache(rid);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    MutablePage page = null;
    try {
      page = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage < 1)
        // CLEANED CORRUPTED RECORD
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      // AVOID DELETION OF CONTENT IN CORRUPTED RECORD
      if (recordPositionInPage < page.getContentSize()) {
        long[] recordSize = page.readNumberAndSize(recordPositionInPage);
        if (recordSize[0] == 0)
          // ALREADY DELETED
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
          // FOUND PLACEHOLDER POINTER: DELETE THE PLACEHOLDER CONTENT FIRST
          final RID placeHolderContentRID = new RID(database, fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
          try {
            deleteRecordInternal(placeHolderContentRID, true, false);
          } catch (RecordNotFoundException e) {
            // PARTIAL RECORD NOT FOUND
          }

        } else if (recordSize[0] == FIRST_CHUNK) {
          // 1ST CHUNK: DELETE ALL THE CHUNKS
          MutablePage chunkPage = page;
          int chunkRecordPositionInPage = recordPositionInPage;
          for (int chunkId = 0; ; ++chunkId) {
            final long nextChunkPointer = chunkPage.readLong(
                (int) (chunkRecordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE));

            if (nextChunkPointer == 0)
              // LAST CHUNK
              break;

            // READ THE NEXT CHUNK
            final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
            final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

            chunkPage = database.getTransaction()
                .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
            chunkRecordPositionInPage = getRecordPositionInPage(chunkPage, chunkPositionInPage);
            if (chunkRecordPositionInPage == 0)
              throw new DatabaseOperationException("Error on fetching multi page record " + rid + " chunk " + chunkId);

            recordSize = chunkPage.readNumberAndSize(chunkRecordPositionInPage);

            if (recordSize[0] != NEXT_CHUNK)
              throw new DatabaseOperationException("Error on fetching multi page record " + rid + " chunk " + chunkId);

            try {
              deleteRecordInternal(new RID(database, fileId, nextChunkPointer), false, true);
            } catch (RecordNotFoundException e) {
              // PARTIAL RECORD NOT FOUND
            }
          }

        } else if (recordSize[0] == NEXT_CHUNK) {
          if (!deleteChunks)
            // CANNOT DELETE A CHUNK DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          if (!deletePlaceholderContent)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
        } else if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
          // WIPE OUT RECORD CONTENT
          try {
            page.writeZeros(recordPositionInPage + 1, (int) (recordSize[0] + recordSize[1] - 1));
          } catch (Exception e) {
            // IGNORE IT
            LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
          }
        }

        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);

        // Track deleted RID to prevent reuse within the same transaction
        database.getTransaction().addDeletedRecord(rid);

        if (recordSize[0] > -1) {
          if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.HIGH.ordinal()) {
            // UPDATE THE STATISTICS
            final PageAnalysis pageAnalysis = new PageAnalysis(page);
            pageAnalysis.totalRecordsInPage = recordCountInPage;
            getFreeSpaceInPage(pageAnalysis);
            updatePageStatistics(pageId, pageAnalysis.spaceAvailableInCurrentPage, (int) ((recordSize[0] + recordSize[1]) * -1));
          } else
            ++changesFromLastStats;
        }

      } else {
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
      }

      LogManager.instance()
          .log(this, Level.FINE, "Deleted record %s (%s threadId=%d)", null, rid, page, Thread.currentThread().threadId());

    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on deletion of record " + rid, e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on deleting record %s content", e, rid);

      if (page != null)
        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
    }
  }

  public void compressPage(final MutablePage page, final boolean forceWipeOut) throws IOException {
    final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

    final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage, false);

    if (orderedRecordContentInPage.isEmpty()) {
      if (recordCountInPage > 0) {
        // RESET RECORD COUNTER TO 0
        page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 0);
        LogManager.instance().log(this, Level.FINE, "Update record count from %d to 0 in page %s", recordCountInPage, page.pageId);
        wipeOutFreeSpace(page, (short) 0);
      }
      return;
    }

    final List<int[]> holes = computePageHoles(orderedRecordContentInPage);

    defragPage(page, holes, recordCountInPage);

    if (!holes.isEmpty()) {
      LogManager.instance().log(this, Level.FINE, "Compressed page %s removed %d holes", page.pageId, holes.size());

      // UPDATE THE RECORD COUNT
      // LAST POSITION IN THE PAGE, UPDATE THE TOTAL RECORDS IN THE PAGE GOING BACK FROM THE CURRENT RECORD
      int newRecordCount = -1;
      for (int i = recordCountInPage - 1; i > -1; i--) {
        final int pos = getRecordPositionInPage(page, i);
        if (pos == 0 || page.readNumberAndSize(pos)[0] == 0)
          // DELETED RECORD
          newRecordCount = i;
        else
          break;
      }

      if (newRecordCount > -1) {
        // UPDATE TOTAL RECORDS IN THE PAGE
        LogManager.instance()
            .log(this, Level.FINE, "Update record count from %d to %d in page %s", recordCountInPage, newRecordCount, page.pageId);
        page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) newRecordCount);
      }

    }

    if (forceWipeOut || !holes.isEmpty())
      wipeOutFreeSpace(page, recordCountInPage);
  }

  private void wipeOutFreeSpace(final MutablePage page, final short recordCountInPage) throws IOException {
    if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
      // WIPE OUT FREE SPACE IN THE PAGE. THIS HELPS WITH THE BACKUP OF DATABASE INCREASING THE COMPRESSION RATE
      try {
        final PageAnalysis pageAnalysis = new PageAnalysis(page);
        pageAnalysis.totalRecordsInPage = recordCountInPage;
        getFreeSpaceInPage(pageAnalysis);

        if (pageAnalysis.spaceAvailableInCurrentPage > 0)
          page.writeZeros(pageAnalysis.newRecordPositionInPage, page.getMaxContentSize() - pageAnalysis.newRecordPositionInPage);
      } catch (Exception e) {
        // IGNORE IT
        LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
      }
    }
  }

  private void defragPage(final MutablePage page, final List<int[]> holes, final short recordCountInPage) {
    int gap = 0;
    for (int i = 0; i < holes.size(); i++) {
      final int[] hole = holes.get(i);
      final int marginEnd = i < holes.size() - 1 ? holes.get(i + 1)[0] : page.getContentSize();

      final int from = hole[0] + hole[1];
      final int to = hole[0] - gap;
      final int length = marginEnd - from;
      if (length < 1)
        LogManager.instance().log(this, Level.SEVERE, "Error on reusing hole in page %s, invalid length %d", page.pageId, length);

      LogManager.instance().log(this, Level.FINE, "Moving segment page %s %d-(%d)->%d...", page.pageId, from, length, to);
      page.move(from, to, length);

      // SHIFT ALL THE POINTERS FROM THE HOLE TO THE LAST
      for (int positionInPage = 0; positionInPage < recordCountInPage; positionInPage++) {
        final int recordPositionInPage = (int) page.readUnsignedInt(
            PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
        if (recordPositionInPage == 0 || recordPositionInPage >= page.getContentSize())
          // AVOID TOUCHING DELETED OR CORRUPTED RECORD
          continue;

        if (recordPositionInPage >= from && recordPositionInPage <= from + length) {
          page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE,
              recordPositionInPage - hole[1] - gap);
          LogManager.instance().log(this, Level.FINE, "- record %d %d->%d", positionInPage, recordPositionInPage,
              recordPositionInPage - hole[1] - gap);
        }
      }

      gap += hole[1];
    }
  }

  private List<int[]> computePageHoles(final List<int[]> orderedRecordContentInPage) {
    final List<int[]> holes = new ArrayList<>(128);

    // USE INT BECAUSE OF THE LIMITED SIZE OF THE PAGE
    int[] lastPointer = new int[] { PAGE_RECORD_TABLE_OFFSET + maxRecordsInPage * INT_SERIALIZED_SIZE, 0 };
    for (int i = 0; i < orderedRecordContentInPage.size(); i++) {
      final int[] pointer = orderedRecordContentInPage.get(i);
      final int lastPointerEnd = lastPointer[0] + lastPointer[1];
      if (pointer[0] != lastPointerEnd) {
        final int[] lastHole = holes.isEmpty() ? null : holes.getLast();
        if (lastHole != null && lastHole[0] + lastHole[1] == pointer[0]) {
          // UPDATE PREVIOUS HOLE
          lastHole[1] += pointer[1];
        } else {
          final int delta = pointer[0] - lastPointerEnd;
          if (delta < 1)
            continue;
          // CREATE A NEW HOLE
          holes.add(new int[] { lastPointerEnd, delta });
        }
      }
      lastPointer = pointer;
    }
    return holes;
  }

  private List<int[]> getOrderedRecordsInPage(BasePage page, final short recordCountInPage, final boolean readOnly) {
    final List<int[]> orderedRecordContentInPage = new ArrayList<>(DEF_MAX_RECORDS_IN_PAGE);

    for (int positionInPage = 0; positionInPage < recordCountInPage; positionInPage++) {
      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      if (recordPositionInPage < 1 || recordPositionInPage >= page.getContentSize())
        // SKIP DELETED RECORD (>=24.1.1), OR CORRUPTED
        continue;

      final int size;
      try {
        final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
        if (recordSize[0] == 0) {
          // DELETED <24.1.1
          if (!readOnly) {
            // SET 0 IN THE RECORD TABLE
            if (!(page instanceof MutablePage))
              page = database.getTransaction().getPageToModify(page);
            ((MutablePage) page).writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
          }
          continue;
        }

        if (recordSize[0] == RECORD_PLACEHOLDER_POINTER)
          size = LONG_SERIALIZED_SIZE + (int) recordSize[1];
        else if (recordSize[0] == FIRST_CHUNK || recordSize[0] == NEXT_CHUNK) {
          final int chunkSize = page.readInt((recordPositionInPage + (int) recordSize[1]));
          size = chunkSize + (int) recordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE; // LONG = nextChunkPointer
        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT)
          // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
          size = (int) (-1 * recordSize[0]) + (int) recordSize[1];
        else
          size = (int) recordSize[0] + (int) recordSize[1];

        if (size < 0 || size > getPageSize() - contentHeaderSize) {
          // INVALID SIZE
          LogManager.instance().log(this, Level.SEVERE,
              "Invalid record size " + size + " for record #" + fileId + ":" + (page.pageId.getPageNumber() * maxRecordsInPage)
                  + positionInPage + ": deleting record");

          if (readOnly) {
            if (!(page instanceof MutablePage))
              page = database.getTransaction().getPageToModify(page);
            ((MutablePage) page).writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
          }
          continue;
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error on loading record #" + fileId + ":" + (page.pageId.getPageNumber() * maxRecordsInPage) + positionInPage);
        continue;
      }

      orderedRecordContentInPage.add(new int[] { recordPositionInPage, size });
    }

    orderedRecordContentInPage.sort(Comparator.comparingLong(a -> a[0]));

    return orderedRecordContentInPage;
  }

  private Binary loadMultiPageRecord(final RID originalRID, BasePage page, int recordPositionInPage, long[] recordSize)
      throws IOException {
    // READ ALL THE CHUNKS TILL THE END
    final Binary record = new Binary();

    while (true) {
      final int chunkSize = page.readInt((int) (recordPositionInPage + recordSize[1]));
      final long nextChunkPointer = page.readLong((int) (recordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE));
      final Binary chunk = page.getImmutableView(
          (int) (recordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE), chunkSize);
      record.append(chunk);

      if (nextChunkPointer == 0)
        // LAST CHUNK
        break;

      final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

      if (chunkPageId >= getTotalPages())
        throw new DatabaseOperationException("Invalid pointer to a chunk for record " + originalRID);

      final BasePage nextPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), chunkPageId), pageSize);

      final int nextRecordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
      if (nextRecordPositionInPage == 0)
        throw new DatabaseOperationException("Chunk of record " + originalRID + " was deleted");

      if (nextPage.equals(page) && recordPositionInPage == nextRecordPositionInPage) {
        // AVOID INFINITE LOOP?
        LogManager.instance().log(this, Level.SEVERE,
            "Infinite loop on loading multi-page record " + originalRID + " chunk " + chunkPageId + "/" + chunkPositionInPage);
        throw new DatabaseOperationException(
            "Infinite loop on loading multi-page record " + originalRID + " chunk " + chunkPageId + "/" + chunkPositionInPage);
      }

      page = nextPage;
      recordPositionInPage = nextRecordPositionInPage;

      recordSize = page.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] != NEXT_CHUNK)
        throw new DatabaseOperationException("Error on fetching multi page record " + originalRID);
    }

    record.position(0);
    return record;
  }

  private int getRecordPositionInPage(final BasePage page, final int positionInPage) throws IOException {
    final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
    if (recordPositionInPage != 0 && recordPositionInPage < contentHeaderSize)
      throw new IOException("Invalid record #" + fileId + ":" + (page.pageId.getPageNumber() * maxRecordsInPage + positionInPage));
    return recordPositionInPage;
  }

  private void writeMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition,
      final int availableSpaceForFirstChunk) throws IOException {
    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK
    int byteWritten = currentPage.writeNumber(newPosition, FIRST_CHUNK);

    newPosition += byteWritten;

    // WRITE CHUNK SIZE
    int chunkSize = availableSpaceForFirstChunk - byteWritten - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
    currentPage.writeInt(newPosition, chunkSize);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    currentPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    int txPageCounter = getTotalPages();
    while (bufferSize > 0) {
      MutablePage nextPage = null;
      int recordIdInPage = 0;

      final int spaceNeededForChunk = bufferSize + 2 + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), spaceNeededForChunk, txPageCounter,
          true);

      if (!pageAnalysis.createNewPage) {
        nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);
        newPosition = pageAnalysis.newRecordPositionInPage;
        recordIdInPage = pageAnalysis.availablePositionIndex;

        nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE, newPosition);

        if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
      }

      if (nextPage == null) {
        // CREATE A NEW PAGE
        nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), txPageCounter++), pageSize);
        newPosition = contentHeaderSize;
        nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
        nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
      }

      // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
      currentPage.writeLong(nextChunkPointerOffset,
          (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);

      int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

      byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
      spaceAvailableInCurrentPage -= byteWritten;

      // WRITE CHUNK SIZE
      chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
      final boolean lastChunk = bufferSize <= chunkSize;
      if (bufferSize < chunkSize)
        // LAST CHUNK, OVERWRITE THE SIZE WITH THE REMAINING CONTENT SIZE
        chunkSize = bufferSize;

      if (chunkSize < 1)
        throw new IllegalArgumentException("Chunk size invalid (" + chunkSize + ")");

      newPosition += byteWritten;
      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      nextChunkPointerOffset = newPosition + INT_SERIALIZED_SIZE;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      updatePageStatistics(nextPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -chunkSize);

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }
  }

  private void updateMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition)
      throws IOException {
    int chunkSize = currentPage.readInt(newPosition);
    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK
    if (bufferSize < chunkSize)
      // 1ST AND LAST CHUNK
      chunkSize = bufferSize;

    currentPage.writeInt(newPosition, chunkSize);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;
    long nextChunkPointer = currentPage.readLong(nextChunkPointerOffset);

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    currentPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    long chunkToDeletePointer = bufferSize > 0 ? 0L : nextChunkPointer;

    while (bufferSize > 0) {
      MutablePage nextPage = null;

      if (nextChunkPointer > 0) {
        final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

        nextPage = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
        final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
        if (recordPositionInPage == 0)
          throw new DatabaseOperationException("Error on fetching multi page record " + originalRID);

        final long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

        if (recordSize[0] != NEXT_CHUNK)
          throw new DatabaseOperationException("Error on fetching multi page record " + originalRID);

        newPosition = (int) (recordPositionInPage + recordSize[1]);
        chunkSize = nextPage.readInt(newPosition);
        nextChunkPointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);

      } else {
        // CREATE NEW SPACE FOR THE CURRENT AND REMAINING CHUNKS
        int recordIdInPage = 0;
        int txPageCounter = getTotalPages();

        final int totalSpaceNeeded = bufferSize + Binary.LONG_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;

        final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), totalSpaceNeeded, txPageCounter,
            true);
        if (!pageAnalysis.createNewPage) {
          // FOUND IT
          nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);
          newPosition = pageAnalysis.newRecordPositionInPage;
          recordIdInPage = pageAnalysis.availablePositionIndex;

          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pageAnalysis.availablePositionIndex * INT_SERIALIZED_SIZE,
              newPosition);

          if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
            nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));

          updatePageStatistics(nextPage.pageId.getPageNumber(), pageAnalysis.spaceAvailableInCurrentPage, -totalSpaceNeeded);
        }

        if (nextPage == null) {
          // CREATE A NEW PAGE
          nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), txPageCounter), pageSize);
          newPosition = contentHeaderSize;
          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
          updatePageStatistics(nextPage.pageId.getPageNumber(), nextPage.getAvailableContentSize(), -bufferSize);
        }

        // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
        currentPage.writeLong(nextChunkPointerOffset,
            (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);
        int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

        final int byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
        spaceAvailableInCurrentPage -= byteWritten;

        chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
        newPosition += byteWritten;
      }

      // WRITE CHUNK SIZE
      final boolean lastChunk = bufferSize <= chunkSize;
      if (bufferSize < chunkSize) {
        // LAST CHUNK
        chunkSize = bufferSize;
        chunkToDeletePointer = nextChunkPointer;
      }

      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      newPosition += INT_SERIALIZED_SIZE;

      nextChunkPointerOffset = newPosition;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }

    // CHECK TO DELETE REMAINING CHUNKS IF THE RECORD SHRUNK
    while (chunkToDeletePointer > 0) {
      currentPage.writeLong(nextChunkPointerOffset, 0L);

      final int chunkPageId = (int) (chunkToDeletePointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (chunkToDeletePointer % maxRecordsInPage);

      final MutablePage nextPage = database.getTransaction()
          .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
      final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);

      // DELETE THE CHUNK AS RECORD IN THE PAGE
      nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE, 0L);

      if (recordPositionInPage == 0) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on deleting extra part of multi-page record %s after it shrunk", originalRID);
        break;
      }

      final long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] != NEXT_CHUNK) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on deleting extra part of multi-page record %s after it shrunk", originalRID);
        break;
      }

      newPosition = (int) (recordPositionInPage + recordSize[1]);
      chunkToDeletePointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);
    }
  }

  private int getLastRecordPositionInPage(final MutablePage page, final int totalRecords) throws IOException {
    int lastRecordPositionInPage = -1;
    for (int i = 0; i < totalRecords; i++) {
      final int recordPositionInPage = getRecordPositionInPage(page, i);
      if (recordPositionInPage != 0 && recordPositionInPage > lastRecordPositionInPage)
        lastRecordPositionInPage = recordPositionInPage;
    }
    return lastRecordPositionInPage;
  }

  /**
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis getAvailableSpaceInPage(final int pageNumber, final int spaceNeeded, final boolean multiPageRecord)
      throws IOException {
    final PageAnalysis result = new PageAnalysis(
        database.getTransaction().getPage(new PageId(database, file.getFileId(), pageNumber), pageSize));

    result.totalRecordsInPage = result.page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    if (result.totalRecordsInPage > 0) {
      getFreeSpaceInPage(result);

      if (!result.createNewPage && result.spaceAvailableInCurrentPage > -1) {
        if (spaceNeeded + SPARE_SPACE_FOR_GROWTH > result.spaceAvailableInCurrentPage)
          // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
          result.createNewPage = true;
      }
    } else if (pageNumber > 0 || !multiPageRecord) {
      // FIRST RECORD, START RIGHT AFTER THE HEADER
      result.availablePositionIndex = 0;
      result.newRecordPositionInPage = contentHeaderSize;
      result.spaceAvailableInCurrentPage = result.page.getMaxContentSize() - result.newRecordPositionInPage;
    } else {
      // NEVER RETURN THE FIRST RECORD IF IT'S A MULTI-PAGE RECORD
      result.createNewPage = true;
    }
    return result;
  }

  private void getFreeSpaceInPage(final PageAnalysis pageAnalysis) throws IOException {
    if (pageAnalysis.totalRecordsInPage >= maxRecordsInPage) {
      pageAnalysis.createNewPage = true;
      return;
    }

    pageAnalysis.availablePositionIndex = -1;
    pageAnalysis.lastRecordPositionInPage = -1;
    pageAnalysis.newRecordPositionInPage = -1;

    for (int i = 0; i < pageAnalysis.totalRecordsInPage; i++) {
      final int recordPositionInPage = getRecordPositionInPage(pageAnalysis.page, i);
      if (recordPositionInPage == 0) {
        // Check if this position was deleted in the current transaction
        final RID potentialRID = new RID(database, file.getFileId(),
            ((long) pageAnalysis.page.getPageId().getPageNumber()) * maxRecordsInPage + i);

        if (!database.getTransaction().isDeletedInTransaction(potentialRID)) {
          // REUSE THE FIRST AVAILABLE POSITION FROM DELETED RECORD (from previous transactions only)
          if (pageAnalysis.availablePositionIndex == -1)
            pageAnalysis.availablePositionIndex = i;
        }
      } else if (recordPositionInPage > pageAnalysis.lastRecordPositionInPage)
        pageAnalysis.lastRecordPositionInPage = recordPositionInPage;
    }

    if (pageAnalysis.availablePositionIndex == -1)
      // USE NEW POSITION
      pageAnalysis.availablePositionIndex = pageAnalysis.totalRecordsInPage;

    if (pageAnalysis.lastRecordPositionInPage == -1) {
      // TOTALLY EMPTY PAGE AFTER DELETION, GET THE FIRST POSITION
      pageAnalysis.newRecordPositionInPage = contentHeaderSize;
    } else {
      final long[] lastRecordSize = pageAnalysis.page.readNumberAndSize(pageAnalysis.lastRecordPositionInPage);
      if (lastRecordSize[0] == 0)
        // DELETED (V<24.1.1)
        pageAnalysis.newRecordPositionInPage = pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[1];
      else if (lastRecordSize[0] > 0)
        // RECORD PRESENT, CONSIDER THE RECORD SIZE + VARINT SIZE
        pageAnalysis.newRecordPositionInPage =
            pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
      else if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER)
        // PLACEHOLDER, CONSIDER NEXT 9 BYTES
        pageAnalysis.newRecordPositionInPage =
            pageAnalysis.lastRecordPositionInPage + LONG_SERIALIZED_SIZE + (int) lastRecordSize[1];
      else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
        // CHUNK
        final int chunkSize = pageAnalysis.page.readInt((int) (pageAnalysis.lastRecordPositionInPage + lastRecordSize[1]));
        pageAnalysis.newRecordPositionInPage =
            pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE
                + chunkSize;
      } else
        // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
        pageAnalysis.newRecordPositionInPage =
            pageAnalysis.lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];
    }
    pageAnalysis.spaceAvailableInCurrentPage = pageAnalysis.page.getMaxContentSize() - pageAnalysis.newRecordPositionInPage;
  }

  /**
   * Find the first page with enough space for the buffer. Algorithm:
   * 1. browse all the pages and find the first page with enough space
   * 2. keep the best page as the page with space available closer to the buffer size
   * 3. if no page has enough space, use the last page
   * 4. if the last page is full, create a new page
   *
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis findAvailableSpace(final int currentPageId, final int spaceNeeded, final int txPageCounter,
      final boolean multiPageRecord)
      throws IOException {
    if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.MEDIUM.ordinal()) {
      synchronized (freeSpaceInPages) {
        if (freeSpaceInPages.isEmpty())
          gatherPageStatistics();

        // TRY WITH THE CURRENT PAGE FIRST
        PageAnalysis bestPageAnalysis = null;

        if (currentPageId > -1) {
          // PRIORITIZE SPACE IN THE SAME PAGE
          bestPageAnalysis = getAvailableSpaceInPage(currentPageId, spaceNeeded, multiPageRecord);
          if (bestPageAnalysis.createNewPage || bestPageAnalysis.totalRecordsInPage > maxRecordsInPage)
            bestPageAnalysis = null;
        }

        if (bestPageAnalysis == null) {
          if (freeSpaceInPages.isEmpty())
            gatherPageStatistics();

          if (!freeSpaceInPages.isEmpty()) {
            bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded, multiPageRecord);
            if (bestPageAnalysis == null)
              // TRY AGAIN WITH HALF SIZE, THE RECORD WILL BE SPLIT IN MULTIPLE CHUNKS
              bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded / 2, multiPageRecord);
          }
        }

        if (bestPageAnalysis != null) {
//        LogManager.instance()
//            .log(this, Level.FINEST, "Requesting %db, allocating in %d/%d (free=%db totalRecords=%d total=%d)", bufferSize,
//                bestAvailableSpace.page.pageId.getFileId(), bestAvailableSpace.page.pageId.getPageNumber(),
//                bestAvailableSpace.spaceAvailableInCurrentPage, bestAvailableSpace.totalRecordsInPage, freeSpaceInPages.size());

          return bestPageAnalysis;
        }
      }
    }

    // CHECK IF THERE IS SPACE IN THE LAST PAGE
    return getAvailableSpaceInPage(txPageCounter - 1, spaceNeeded, multiPageRecord);
  }

  /**
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis findAvailableSpaceFromStatistics(final int currentPageId, final int spaceNeeded,
      final boolean multiPageRecord)
      throws IOException {
    PageAnalysis bestPageAnalysis = null;
    List<Integer> pagesToRemove = null;
    for (Map.Entry<Integer, Integer> entry : freeSpaceInPages.entrySet()) {
      final int pageId = entry.getKey();
      if (pageId == currentPageId)
        // ALREADY EVALUATED
        continue;

      final Integer pageStats = entry.getValue();

      if (pageStats >= spaceNeeded) {
        // CHECK IF THE SPACE AVAILABLE IS REAL
        final PageAnalysis pageAnalysis = getAvailableSpaceInPage(pageId, spaceNeeded, multiPageRecord);

        if (pageAnalysis.totalRecordsInPage >= maxRecordsInPage) {
          pagesToRemove = pagesToRemove == null ? new ArrayList<>() : pagesToRemove;
          pagesToRemove.add(pageId);
          continue;
        }

        if (!pageAnalysis.createNewPage) {
          if (pageAnalysis.spaceAvailableInCurrentPage != pageStats) {
            // LOW COST UPDATE OF STATISTICS
            if (pageAnalysis.spaceAvailableInCurrentPage < MINIMUM_SPACE_LEFT_IN_PAGE) {
              pagesToRemove = pagesToRemove == null ? new ArrayList<>() : pagesToRemove;
              pagesToRemove.add(pageId);
            } else
              entry.setValue(pageAnalysis.spaceAvailableInCurrentPage);
          }

          if (multiPageRecord && pageAnalysis.page.pageId.getPageNumber() == 0 && pageAnalysis.availablePositionIndex == 0)
            // AVOID REUSING THE FIRST RECORD IF IT'S A MULTI-PAGE RECORD
            continue;

          bestPageAnalysis = pageAnalysis;
          break;
        }
      }
    }

    if (pagesToRemove != null)
      for (Integer pageId : pagesToRemove)
        freeSpaceInPages.remove(pageId);

    return bestPageAnalysis;
  }

  /**
   * Gather statistics about the free space in pages. Algorithm:
   * 1. browse all the pages but the latest, and add in a tree map the pages with enough free space (>GATHER_STATS_MIN_SPACE_PERC)
   * 2. save in the map recordCountInPage and freeSpaceInPage
   * 3. if the tree map is full (size > MAX_PAGES_GATHER_STATS), stop
   */
  public void gatherPageStatistics() {
    if (timeOfLastStats == 0L || (System.currentTimeMillis() - timeOfLastStats > MAX_TIMEOUT_GATHER_STATS
        && changesFromLastStats > 0))
      try {
        int txPageCount = getTotalPages();

        synchronized (freeSpaceInPages) {
          for (int pageId = 0; pageId < txPageCount - 2; ++pageId) {
            final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
            final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
            final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage, true);

            int freeSpaceInPage = getPageSize() - contentHeaderSize;
            if (!orderedRecordContentInPage.isEmpty()) {
              final int[] lastRecord = orderedRecordContentInPage.getLast();
              freeSpaceInPage = getPageSize() - (lastRecord[0] + lastRecord[1]);
            }

            final int freeSpacePerc = freeSpaceInPage * 100 / (getPageSize() - contentHeaderSize);

            if (freeSpacePerc > GATHER_STATS_MIN_SPACE_PERC)
              freeSpaceInPages.put(pageId, freeSpaceInPage);

            if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS)
              break;
          }

          timeOfLastStats = System.currentTimeMillis();
          changesFromLastStats = 0L;
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error on gathering statistics on bucket '%s'", e, getName());
      }
  }

  /**
   * Update the in memory statistics about the free space in page.
   */
  private void updatePageStatistics(final int pageId, final int availableSpace, final int delta) {
    ++changesFromLastStats;

    if (reuseSpaceMode.ordinal() < REUSE_SPACE_MODE.HIGH.ordinal())
      return;

    synchronized (freeSpaceInPages) {
      if (availableSpace + delta == 0)
        freeSpaceInPages.remove(pageId);
      else {
        final int usableSpaceInPage = getPageSize() - contentHeaderSize;

        final Integer freeSpaceInPage = freeSpaceInPages.get(pageId);
        final int prevSpace = availableSpace == 0 && freeSpaceInPage != null ? freeSpaceInPage : availableSpace;
        final int newSpace = prevSpace + delta;

        if (freeSpaceInPage != null) {
          if (newSpace <= MINIMUM_SPACE_LEFT_IN_PAGE || (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS
              && newSpace * 100 / usableSpaceInPage < GATHER_STATS_MIN_SPACE_PERC))
            freeSpaceInPages.remove(pageId);
          else
            freeSpaceInPages.put(pageId, newSpace);
        } else if (newSpace * 100 / usableSpaceInPage >= GATHER_STATS_MIN_SPACE_PERC) {
          if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS) {
            // REMOVE THE SMALLEST PAGE
            int lowestPageId = -1;
            int lowestPageSpace = -1;

            for (Map.Entry<Integer, Integer> entry : freeSpaceInPages.entrySet()) {
              if (lowestPageId < 0 || entry.getValue() < lowestPageSpace) {
                lowestPageId = entry.getKey();
                lowestPageSpace = entry.getValue();
              }
            }

            if (lowestPageId > -1 && lowestPageSpace < newSpace) {
              freeSpaceInPages.remove(lowestPageId);
              freeSpaceInPages.put(pageId, newSpace);
            }
          } else
            freeSpaceInPages.put(pageId, newSpace);
        }
      }
    }
  }

  public void setPageStatistics(final JSONArray pages) {
    synchronized (freeSpaceInPages) {
      freeSpaceInPages.clear();
      for (int i = 0; i < pages.length(); i++) {
        final JSONObject page = pages.getJSONObject(i);
        freeSpaceInPages.put(page.getInt("id"), page.getInt("free"));
      }
    }
  }

  public JSONObject getStatistics() {
    final JSONObject json = new JSONObject();

    final long cachedCount = getCachedRecordCount();
    if (cachedCount > -1)
      json.put("count", cachedCount);

    final JSONArray pages = new JSONArray();
    synchronized (freeSpaceInPages) {
      for (Map.Entry<Integer, Integer> entry : freeSpaceInPages.entrySet())
        pages.put(//
            new JSONObject().put("id", entry.getKey()).put("free", entry.getValue())//
        );
    }
    json.put("pages", pages);

    return json;
  }
}
