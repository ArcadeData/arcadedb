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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.LONG_SERIALIZED_SIZE;

/**
 * PAGE CONTENT = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(2048*uint=8192)]
 * <br><br>
 * Record size is the length of the record:
 * <ul>
 * <li>0 = deleted record</li>
 * <li>-1 = placeholder pointer that points to another record in another page</li>
 * <li><-1 = placeholder content pointed from another record in another page</li>
 * </ul>
 * The record size is stored as a varint (variable integer size). The minimum size of a record stored in a page is 5 bytes. If the record is smaller than 5 bytes,
 * it is filled with blanks.
 */
public class Bucket extends PaginatedComponent {
  public static final    String BUCKET_EXT                       = "bucket";
  public static final    int    DEF_PAGE_SIZE                    = 65536;
  public static final    int    CURRENT_VERSION                  = 0;
  public static final    long   RECORD_PLACEHOLDER_POINTER       = -1L;    // USE -1 AS SIZE TO STORE A PLACEHOLDER (THAT POINTS TO A RECORD ON ANOTHER PAGE)
  public static final    long   FIRST_CHUNK                      = -2L;    // USE -2 TO MARK THE FIRST CHUNK OF A BIG RECORD. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK
  public static final    long   NEXT_CHUNK                       = -3L;    // USE -3 TO MARK THE SECOND AND FURTHER CHUNK THAT IS PART OF A BIG RECORD THAT DOES NOT FIT A PAGE. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK OR 0 IF THE CURRENT CHUNK IS THE LAST (NO FURTHER CHUNKS)
  protected static final int    PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int    PAGE_RECORD_TABLE_OFFSET         = PAGE_RECORD_COUNT_IN_PAGE_OFFSET + Binary.SHORT_SERIALIZED_SIZE;
  private static final   int    DEF_MAX_RECORDS_IN_PAGE          = 2048;
  private static final   int    MINIMUM_RECORD_SIZE              = 5;    // RECORD SIZE CANNOT BE < 5 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER
  private static final   long   RECORD_PLACEHOLDER_CONTENT       = MINIMUM_RECORD_SIZE * -1L;    // < -5 FOR SURROGATE RECORDS
  private static final   long   MINIMUM_SPACE_LEFT_IN_PAGE       = 50L;
  protected final        int    contentHeaderSize;
  private final          int    maxRecordsInPage                 = DEF_MAX_RECORDS_IN_PAGE;

  private static class AvailableSpace {
    public BasePage page              = null;
    public int      newPosition       = -1;
    public int      recordCountInPage = -1;
    public boolean  createNewPage     = false;
  }

  public static class PaginatedComponentFactoryHandler implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new Bucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * Called at creation time.
   */
  public Bucket(final DatabaseInternal database, final String name, final String filePath, final PaginatedFile.MODE mode, final int pageSize, final int version)
      throws IOException {
    super(database, name, filePath, BUCKET_EXT, mode, pageSize, version);
    contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
  }

  /**
   * Called at load time.
   */
  public Bucket(final DatabaseInternal database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode, final int pageSize,
      final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
  }

  public int getMaxRecordsInPage() {
    return maxRecordsInPage;
  }

  public RID createRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    return createRecordInternal(record, false, discardRecordAfter);
  }

  public void updateRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);
    updateRecordInternal(record, record.getIdentity(), false, discardRecordAfter);
  }

  public Binary getRecord(final RID rid) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Binary rec = getRecordInternal(rid, false);
    if (rec == null)
      // DELETED
      throw new RecordNotFoundException("Record " + rid + " not found", rid);
    return rec;
  }

  public boolean existsRecord(final RID rid) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        return false;
    }

    try {
      final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        return false;

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      if (recordPositionInPage == 0)
        // CLEANED CORRUPTED RECORD
        return false;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

      return recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK;

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  public void deleteRecord(final RID rid) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false);
  }

  public void scan(final RawRecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final RID rid = new RID(database, id, ((long) pageId) * maxRecordsInPage + recordIdInPage);

            try {
              int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
              if (recordPositionInPage == 0)
                // CLEANED CORRUPTED RECORD
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
                final RID placeHolderPointer = new RID(database, id, page.readLong((int) (recordPositionInPage + recordSize[1])));
                final Binary view = getRecordInternal(placeHolderPointer, true);
                if (view != null && !callback.onRecord(rid, view))
                  return;
              } else if (recordSize[0] == FIRST_CHUNK) {
                // LOAD THE ENTIRE RECORD IN CHUNKS
                final Binary view = loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
                if (view != null && !callback.onRecord(rid, view))
                  return;
              }

            } catch (ArcadeDBException e) {
              throw e;
            } catch (Exception e) {
              if (errorRecordCallback == null)
                LogManager.instance().log(this, Level.SEVERE, String.format("Error on loading record #%s (error: %s)", rid, e.getMessage()));
              else if (!errorRecordCallback.onErrorLoading(rid, e))
                return;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot scan bucket '" + name + "'", e);
    }
  }

  public void fetchPageInTransaction(final RID rid) throws IOException {
    if (rid.getPosition() < 0L) {
      LogManager.instance().log(this, Level.WARNING, "Cannot load a page from a record with invalid RID (" + rid + ")");
      return;
    }

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount) {
        LogManager.instance().log(this, Level.WARNING, "Record " + rid + " not found");
      }
    }

    database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageId), pageSize, false);
  }

  public Iterator<Record> iterator() {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.READ_RECORD);
    return new BucketIterator(this, database);
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof Bucket))
      return false;

    return ((Bucket) obj).id == this.id;
  }

  @Override
  public int hashCode() {
    return id;
  }

  public long count() {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.READ_RECORD);

    long total = 0;

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
            if (recordPositionInPage == 0)
              // CLEANED CORRUPTED RECORD
              continue;

            final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

            if (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK)
              total++;

          }
        }
      }
    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot count bucket '" + name + "'", e);
    }
    return total;
  }

  protected Map<String, Object> check(final int verboseLevel, final boolean fix) {
    final Map<String, Object> stats = new HashMap<>();

    final int totalPages = getTotalPages();

    if (verboseLevel > 1)
      LogManager.instance().log(this, Level.INFO, "- Checking bucket '%s' (totalPages=%d spaceOnDisk=%s pageSize=%s)...", name, totalPages,
          FileUtils.getSizeAsString((long) totalPages * pageSize), FileUtils.getSizeAsString(pageSize));

    long totalAllocatedRecords = 0L;
    long totalActiveRecords = 0L;
    long totalPlaceholderRecords = 0L;
    long totalSurrogateRecords = 0L;
    long totalMultiPageeRecords = 0L;
    long totalDeletedRecords = 0L;
    long totalMaxOffset = 0L;
    long totalChunks = 0L;

    long errors = 0L;
    final List<String> warnings = new ArrayList<>();
    final List<RID> deletedRecords = new ArrayList<>();

    String warning = null;

    for (int pageId = 0; pageId < totalPages; ++pageId) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        int pageActiveRecords = 0;
        int pagePlaceholderRecords = 0;
        int pageSurrogateRecords = 0;
        int pageDeletedRecords = 0;
        int pageMaxOffset = 0;
        int pageMultiPageRecords = 0;
        int pageChunks = 0;

        for (int positionInPage = 0; positionInPage < recordCountInPage; ++positionInPage) {
          final RID rid = new RID(database, file.getFileId(), positionInPage);

          final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);

          if (recordPositionInPage == 0) {
            // CLEANED CORRUPTED RECORD
            pageDeletedRecords++;
            totalDeletedRecords++;

          } else if (recordPositionInPage > page.getContentSize()) {
            ++errors;
            warning = String.format("invalid record offset %d in page for record %s", recordPositionInPage, rid);
            if (fix) {
              deleteRecord(rid);
              deletedRecords.add(rid);
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
                totalMultiPageeRecords++;
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
                ++errors;
                warning = String.format("wrong record size %d found for record %s", recordSize[1] + recordSize[0], rid);
                if (fix) {
                  deleteRecord(rid);
                  deletedRecords.add(rid);
                  ++totalDeletedRecords;
                }
              }

              if (endPosition > pageMaxOffset)
                pageMaxOffset = (int) endPosition;

            } catch (Exception e) {
              ++errors;
              warning = String.format("unknown error on loading record %s: %s", rid, e.getMessage());
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
          LogManager.instance()
              .log(this, Level.FINE, "-- Page %d records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d multiPageRecords=%d chunks=%d) maxOffset=%d",
                  pageId, recordCountInPage, pageActiveRecords, pageDeletedRecords, pagePlaceholderRecords, pageSurrogateRecords, pageMultiPageRecords,
                  pageChunks, pageMaxOffset);

      } catch (Exception e) {
        ++errors;
        warning = String.format("unknown error on checking page %d: %s", pageId, e.getMessage());
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
          .log(this, Level.INFO, "-- Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%", totalAllocatedRecords,
              totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords, avgPageUsed);

    stats.put("pageSize", (long) pageSize);
    stats.put("totalPages", (long) totalPages);
    stats.put("totalAllocatedRecords", totalAllocatedRecords);
    stats.put("totalActiveRecords", totalActiveRecords);
    stats.put("totalPlaceholderRecords", totalPlaceholderRecords);
    stats.put("totalSurrogateRecords", totalSurrogateRecords);
    stats.put("totalDeletedRecords", totalDeletedRecords);
    stats.put("totalMaxOffset", totalMaxOffset);

    DocumentType type = database.getSchema().getTypeByBucketId(id);
    if (type instanceof VertexType) {
      stats.put("totalAllocatedVertices", totalAllocatedRecords);
      stats.put("totalActiveVertices", totalActiveRecords);
    } else if (type instanceof EdgeType) {
      stats.put("totalAllocatedEdges", totalAllocatedRecords);
      stats.put("totalActiveEdges", totalActiveRecords);
    }

    stats.put("deletedRecords", deletedRecords);
    stats.put("warnings", warnings);
    stats.put("autoFix", 0L);
    stats.put("errors", errors);

    return stats;
  }

  Binary getRecordInternal(final RID rid, final boolean readPlaceHolderContent) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      if (recordPositionInPage == 0)
        // CLEANED CORRUPTED RECORD
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
        final RID placeHolderPointer = new RID(database, rid.getBucketId(), page.readLong((int) (recordPositionInPage + recordSize[1])));
        return getRecordInternal(placeHolderPointer, true);
      } else if (recordSize[0] == FIRST_CHUNK) {
        // FOUND 1ST CHUNK, LOAD THE ENTIRE MULTI-PAGE RECORD
        return loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
      } else if (recordSize[0] == NEXT_CHUNK)
        // CANNOT LOAD PARTIAL CHUNK
        return null;

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on lookup of record " + rid, e);
    }
  }

  private RID createRecordInternal(final Record record, final boolean isPlaceHolder, final boolean discardRecordAfter) {
    final Binary buffer = database.getSerializer().serialize(database, record);

    // RECORD SIZE CANNOT BE < 5 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER. FILL THE DIFFERENCE WITH BLANK (0)
    while (buffer.size() < MINIMUM_RECORD_SIZE)
      buffer.putByte(buffer.size() - 1, (byte) 0);

    int bufferSize = buffer.size();

    try {
      int newPosition = -1;
      int recordCountInPage = -1;
      boolean createNewPage;
      BasePage lastPage = null;

      int txPageCounter = getTotalPages();

      if (txPageCounter > 0) {
        // CHECK IF THERE IS SPACE IN THE LAST PAGE
        final AvailableSpace availableSpace = checkForSpaceInPage(txPageCounter - 1, isPlaceHolder, bufferSize);

        lastPage = availableSpace.page;
        newPosition = availableSpace.newPosition;
        recordCountInPage = availableSpace.recordCountInPage;
        createNewPage = availableSpace.createNewPage;

      } else
        createNewPage = true;

      MutablePage selectedPage;
      if (createNewPage) {
        selectedPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);
        newPosition = contentHeaderSize;
        recordCountInPage = 0;
      } else
        selectedPage = database.getTransaction().getPageToModify(lastPage.pageId, pageSize, false);

      final RID rid = new RID(database, file.getFileId(), ((long) selectedPage.getPageId().getPageNumber()) * maxRecordsInPage + recordCountInPage);

      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newPosition;
      final int spaceNeeded = Binary.getNumberSpace(isPlaceHolder ? (-1L * bufferSize) : bufferSize) + bufferSize;

      if (spaceNeeded > spaceAvailableInCurrentPage) {
        // MULTI-PAGE RECORD
        writeMultiPageRecord(buffer, selectedPage, newPosition, spaceAvailableInCurrentPage);

      } else {
        final int byteWritten = selectedPage.writeNumber(newPosition, isPlaceHolder ? (-1L * bufferSize) : bufferSize);
        selectedPage.writeByteArray(newPosition + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
      }

      selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordCountInPage * INT_SERIALIZED_SIZE, newPosition);
      selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) ++recordCountInPage);

      LogManager.instance()
          .log(this, Level.FINE, "Created record %s (page=%s records=%d threadId=%d)", rid, selectedPage, recordCountInPage, Thread.currentThread().getId());

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return rid;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + name + "'", e);
    }
  }

  private boolean updateRecordInternal(final Record record, final RID rid, final boolean updatePlaceholderContent, final boolean discardRecordAfter) {
    if (rid.getPosition() < 0)
      throw new IllegalArgumentException("Cannot update a record with invalid RID");

    final Binary buffer = database.getSerializer().serialize(database, record);

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final MutablePage page = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageId), pageSize, false);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      if (recordPositionInPage == 0)
        // CLEANED CORRUPTED RECORD
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == 0L)
        // DELETED
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      boolean isPlaceHolder = false;
      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {

        // FOUND A RECORD POINTED FROM A PLACEHOLDER
        final RID placeHolderContentRID = new RID(database, id, page.readLong((int) (recordPositionInPage + recordSize[1])));
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

      int bufferSize = buffer.size();
      if (bufferSize > recordSize[0]) {
        // UPDATED RECORD IS LARGER THAN THE PREVIOUS VERSION: MAKE ROOM IN THE PAGE IF POSSIBLE

        final int lastRecordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (recordCountInPage - 1) * INT_SERIALIZED_SIZE);
        final long[] lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);

        if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER) {
          lastRecordSize[0] = LONG_SERIALIZED_SIZE;
          lastRecordSize[1] = 1L;
        } else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
          // CONSIDER THE CHUNK SIZE
          lastRecordSize[0] = page.readInt((int) (lastRecordPositionInPage + recordSize[1]));
          lastRecordSize[1] = INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
        } else if (lastRecordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          lastRecordSize[0] *= -1L;
        }

        final int pageOccupiedInBytes = (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]) + 1;
        int spaceAvailableInCurrentPage = page.getMaxContentSize() - pageOccupiedInBytes - 1;
        final int bufferSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int additionalSpaceNeeded = (int) (bufferSize + bufferSizeLength - recordSize[0] - recordSize[1]);

        if (additionalSpaceNeeded <= spaceAvailableInCurrentPage) {
          // THERE IS SPACE LEFT IN THE PAGE, SHIFT ON THE RIGHT THE EXISTENT RECORDS
          if (positionInPage < recordCountInPage - 1) {
            // NOT LAST RECORD IN PAGE, SHIFT NEXT RECORDS
            final int nextRecordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (positionInPage + 1) * INT_SERIALIZED_SIZE);

            final int newPos = nextRecordPositionInPage + additionalSpaceNeeded;

            page.move(nextRecordPositionInPage, newPos, pageOccupiedInBytes - nextRecordPositionInPage);

            // TODO: CALCULATE THE REAL SIZE TO COMPACT DELETED RECORDS/PLACEHOLDERS
            for (int pos = positionInPage + 1; pos < recordCountInPage; ++pos) {
              final int nextRecordPosInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE);

              if (nextRecordPosInPage == 0)
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, 0);
              else
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, nextRecordPosInPage + additionalSpaceNeeded);

              assert nextRecordPosInPage + additionalSpaceNeeded < page.getMaxContentSize();
            }
          }

          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

          page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

          LogManager.instance().log(this, Level.FINE, "Updated record %s by allocating new space on the same page (page=%s threadId=%d)", null, rid, page,
              Thread.currentThread().getId());

        } else {
          if (isPlaceHolder)
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            return false;

          final int availableSpaceForChunk = (int) (recordSize[0] + recordSize[1]);

          if (availableSpaceForChunk < 2 + LONG_SERIALIZED_SIZE + INT_SERIALIZED_SIZE) {
            final int bytesWritten = page.writeNumber(recordPositionInPage, RECORD_PLACEHOLDER_POINTER);

            final RID realRID = createRecordInternal(record, true, false);
            page.writeLong(recordPositionInPage + bytesWritten, realRID.getPosition());

            LogManager.instance().log(this, Level.FINE, "Updated record %s by allocating new space with a placeholder (page=%s threadId=%d)", null, rid, page,
                Thread.currentThread().getId());
          } else {
            // SPLIT THE RECORD IN CHUNKS AS LINKED LIST AND STORE THE FIRST PART ON CURRENT PAGE ISSUE https://github.com/ArcadeData/arcadedb/issues/332
            writeMultiPageRecord(buffer, page, recordPositionInPage, availableSpaceForChunk);

            LogManager.instance()
                .log(this, Level.FINE, "Updated record %s by splitting it in multiple chunks to be saved in multiple pages (page=%s threadId=%d)", null, rid,
                    page, Thread.currentThread().getId());
          }
        }
      } else {
        // UPDATED RECORD CONTENT IS NOT LARGER THAN PREVIOUS VERSION: OVERWRITE THE CONTENT
        recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
        page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

        LogManager.instance().log(this, Level.FINE, "Updated record %s with the same size or less as before (page=%s threadId=%d)", null, rid, page,
            Thread.currentThread().getId());
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return true;

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on update record " + rid, e);
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholderContent, final boolean deleteChunks) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    database.getTransaction().removeRecordFromCache(rid);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final MutablePage page = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageId), pageSize, false);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
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
          final RID placeHolderContentRID = new RID(database, id, page.readLong((int) (recordPositionInPage + recordSize[1])));
          deleteRecordInternal(placeHolderContentRID, true, false);

        } else if (recordSize[0] == FIRST_CHUNK) {
          // 1ST CHUNK: DELETE ALL THE CHUNKS
          MutablePage chunkPage = page;
          int chunkRecordPositionInPage = recordPositionInPage;
          for (int chunkId = 0; ; ++chunkId) {
            final long nextChunkPointer = chunkPage.readLong((int) (chunkRecordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE));
            if (nextChunkPointer == 0)
              // LAST CHUNK
              break;

            // READ THE NEXT CHUNK
            final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
            final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

            chunkPage = database.getTransaction().getPage(new PageId(file.getFileId(), chunkPageId), pageSize).modify();
            chunkRecordPositionInPage = (int) chunkPage.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE);
            recordSize = chunkPage.readNumberAndSize(chunkRecordPositionInPage);

            if (recordSize[0] != NEXT_CHUNK)
              throw new DatabaseOperationException("Error on fetching multi page record " + rid + " chunk " + chunkId);

            deleteRecordInternal(new RID(database, id, nextChunkPointer), false, true);
          }

        } else if (recordSize[0] == NEXT_CHUNK) {
          if (!deleteChunks)
            // CANNOT DELETE A CHUNK DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          if (!deletePlaceholderContent)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
        }

        // CONTENT SIZE = 0 MEANS DELETED
        page.writeNumber(recordPositionInPage, 0L);

      } else {
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
      }

      // TODO: EVALUATE COMPACTING THE PAGE FOR REUSING THE SPACE
//      recordPositionInPage++;
//
//      AVOID COMPACTION DURING DELETE
//      // COMPACT PAGE BY SHIFTING THE RECORDS TO THE LEFT
//      for (int pos = positionInPage + 1; pos < recordCountInPage; ++pos) {
//        final int nextRecordPosInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE);
//        final byte[] record = page.readBytes(nextRecordPosInPage);
//
//        final int bytesWritten = page.writeBytes(recordPositionInPage, record);
//
//        // OVERWRITE POS TABLE WITH NEW POSITION
//        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, recordPositionInPage);
//
//        recordPositionInPage += bytesWritten;
//      }
//
//      page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordCountInPage - 1));

      LogManager.instance().log(this, Level.FINE, "Deleted record %s (page=%s threadId=%d)", null, rid, page, Thread.currentThread().getId());

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on deletion of record " + rid, e);
    }
  }

  private Binary loadMultiPageRecord(final RID originalRID, BasePage page, int recordPositionInPage, long[] recordSize) throws IOException {
    // READ ALL THE CHUNKS TILL THE END
    final Binary record = new Binary();

    while (true) {
      final int chunkSize = page.readInt((int) (recordPositionInPage + recordSize[1]));
      final long nextChunkPointer = page.readLong((int) (recordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE));
      final Binary chunk = page.getImmutableView((int) (recordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE), chunkSize);
      record.append(chunk);

      if (nextChunkPointer == 0)
        // LAST CHUNK
        break;

      final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

      page = database.getTransaction().getPage(new PageId(file.getFileId(), chunkPageId), pageSize);
      recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE);
      recordSize = page.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] != NEXT_CHUNK)
        throw new DatabaseOperationException("Error on fetching multi page record " + originalRID);
    }

    record.position(0);
    return record;
  }

  private void writeMultiPageRecord(final Binary buffer, MutablePage currentPage, int newPosition, final int availableSpaceForFirstChunk) throws IOException {
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
      if (currentPage.getPageId().getPageNumber() < txPageCounter - 1) {
        // LOOK IN THE LAST PAGE FOR SPACE
        final AvailableSpace availableSpace = checkForSpaceInPage(txPageCounter - 1, false, bufferSize);
        if (!availableSpace.createNewPage) {
          nextPage = database.getTransaction().getPageToModify(availableSpace.page.pageId, pageSize, false);
          newPosition = availableSpace.newPosition;
          recordIdInPage = availableSpace.recordCountInPage;

          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availableSpace.recordCountInPage * INT_SERIALIZED_SIZE, newPosition);
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
        }
      }

      if (nextPage == null) {
        // CREATE A NEW PAGE
        nextPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter++), pageSize);
        newPosition = contentHeaderSize;
        nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
        nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
      }

      // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
      currentPage.writeLong(nextChunkPointerOffset, (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);

      int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

      byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
      spaceAvailableInCurrentPage -= byteWritten;

      // WRITE CHUNK SIZE
      chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
      final boolean lastChunk = bufferSize < chunkSize;
      if (lastChunk)
        // LAST CHUNK
        chunkSize = bufferSize;

      newPosition += byteWritten;
      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      nextChunkPointerOffset = newPosition + INT_SERIALIZED_SIZE;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }
  }

  private void updateMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition) throws IOException {
    int chunkSize = currentPage.readInt(newPosition);

    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK
    if (bufferSize < chunkSize)
      // LAST CHUNK
      chunkSize = bufferSize;

    // WRITE CHUNK SIZE
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
    int txPageCounter = getTotalPages();

    while (bufferSize > 0) {
      MutablePage nextPage = null;

      if (nextChunkPointer > 0) {
        final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

        nextPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), chunkPageId), pageSize, false);
        final int recordPositionInPage = (int) nextPage.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE);
        long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

        if (recordSize[0] != NEXT_CHUNK)
          throw new DatabaseOperationException("Error on fetching multi page record " + originalRID);

        newPosition = (int) (recordPositionInPage + recordSize[1]);
        chunkSize = nextPage.readInt(newPosition);
        nextChunkPointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);

      } else {
        // CREATE NEW SPACE FOR THE CURRENT AND REMAINING CHUNKS
        int recordIdInPage = 0;
        if (currentPage.getPageId().getPageNumber() < txPageCounter - 1) {
          // LOOK IN THE LAST PAGE FOR SPACE
          final AvailableSpace availableSpace = checkForSpaceInPage(txPageCounter - 1, false, bufferSize);
          if (!availableSpace.createNewPage) {
            nextPage = database.getTransaction().getPageToModify(availableSpace.page.pageId, pageSize, false);
            newPosition = availableSpace.newPosition;
            recordIdInPage = availableSpace.recordCountInPage;

            nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availableSpace.recordCountInPage * INT_SERIALIZED_SIZE, newPosition);
            nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
          }
        }

        if (nextPage == null) {
          // CREATE A NEW PAGE
          nextPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter++), pageSize);
          newPosition = contentHeaderSize;
          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
        }

        // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
        currentPage.writeLong(nextChunkPointerOffset, (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);
        int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

        final int byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
        spaceAvailableInCurrentPage -= byteWritten;
        chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
        newPosition += byteWritten;
      }

      // WRITE CHUNK SIZE
      final boolean lastChunk = bufferSize < chunkSize;
      if (lastChunk)
        // LAST CHUNK
        chunkSize = bufferSize;

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
  }

  private AvailableSpace checkForSpaceInPage(final int pageNumber, final boolean isPlaceHolder, final int bufferSize) throws IOException {
    final AvailableSpace result = new AvailableSpace();

    result.page = database.getTransaction().getPage(new PageId(file.getFileId(), pageNumber), pageSize);

    result.recordCountInPage = result.page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    if (result.recordCountInPage >= maxRecordsInPage)
      // MAX NUMBER OF RECORDS IN A PAGE REACHED, USE A NEW PAGE
      result.createNewPage = true;
    else if (result.recordCountInPage > 0) {
      // GET FIRST EMPTY POSITION
      final int lastRecordPositionInPage = (int) result.page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (result.recordCountInPage - 1) * INT_SERIALIZED_SIZE);
      if (lastRecordPositionInPage == 0)
        // CLEANED CORRUPTED RECORD
        result.createNewPage = true;
      else {
        final long[] lastRecordSize = result.page.readNumberAndSize(lastRecordPositionInPage);

        if (lastRecordSize[0] > 0)
          // RECORD PRESENT, CONSIDER THE RECORD SIZE + VARINT SIZE
          result.newPosition = lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
        else if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER)
          // PLACEHOLDER, CONSIDER NEXT 9 BYTES
          result.newPosition = lastRecordPositionInPage + LONG_SERIALIZED_SIZE + 1;
        else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
          // CHUNK
          final int chunkSize = result.page.readInt((int) (lastRecordPositionInPage + lastRecordSize[1]));
          result.newPosition = lastRecordPositionInPage + (int) lastRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + chunkSize;
        } else
          // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
          result.newPosition = lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];

        final long spaceAvailableInCurrentPage = result.page.getMaxContentSize() - result.newPosition;
        int spaceNeeded = Binary.getNumberSpace(isPlaceHolder ? (-1L * bufferSize) : bufferSize) + bufferSize;
        if (spaceNeeded > spaceAvailableInCurrentPage && spaceAvailableInCurrentPage < MINIMUM_SPACE_LEFT_IN_PAGE)
          // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
          result.createNewPage = true;
      }
    } else
      // FIRST RECORD, START RIGHT AFTER THE HEADER
      result.newPosition = contentHeaderSize;

    return result;
  }
}
