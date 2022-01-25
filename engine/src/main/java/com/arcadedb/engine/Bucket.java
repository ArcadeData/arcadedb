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
 * <br>
 * Record size is the length of the record or -1 if a placeholder is stored and -2 for the placeholder itself.
 */
public class Bucket extends PaginatedComponent {
  public static final    String BUCKET_EXT                       = "bucket";
  public static final    int    DEF_PAGE_SIZE                    = 65536;
  public static final    int    CURRENT_VERSION                  = 0;
  protected static final int    PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int    PAGE_RECORD_TABLE_OFFSET         = PAGE_RECORD_COUNT_IN_PAGE_OFFSET + Binary.SHORT_SERIALIZED_SIZE;
  private static final   int    DEF_MAX_RECORDS_IN_PAGE          = 2048;

  protected final int contentHeaderSize;
  private final   int maxRecordsInPage = DEF_MAX_RECORDS_IN_PAGE;

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

  public RID createRecord(final Record record) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    return createRecordInternal(record, false);
  }

  public void updateRecord(final Record record) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);
    updateRecordInternal(record, record.getIdentity(), false);
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

      return recordSize[0] > 0 || recordSize[0] == -1;

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  public void deleteRecord(final RID rid) {
    database.checkPermissionsOnFile(id, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false);
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
              final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
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

              } else if (recordSize[0] == -1) {
                // PLACEHOLDER
                final Binary view = getRecordInternal(new RID(database, id, page.readLong((int) (recordPositionInPage + recordSize[1]))), true);
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

            if (recordSize[0] > 0 || recordSize[0] == -1)
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
    long totalDeletedRecords = 0L;
    long totalMaxOffset = 0L;
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
              } else if (recordSize[0] < -1) {
                pageSurrogateRecords++;
                totalSurrogateRecords++;
                recordSize[0] *= -1;
              } else if (recordSize[0] == -1) {
                pagePlaceholderRecords++;
                totalPlaceholderRecords++;
                recordSize[0] = 5;
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
              .log(this, Level.FINE, "-- Page %d records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) maxOffset=%d", pageId, recordCountInPage,
                  pageActiveRecords, pageDeletedRecords, pagePlaceholderRecords, pageSurrogateRecords, pageMaxOffset);

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

  Binary getRecordInternal(final RID rid, final boolean readPlaceHolder) {
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

      if (recordSize[0] < -1) {
        if (!readPlaceHolder)
          // PLACEHOLDER
          return null;

        recordSize[0] *= -1;
      }

      if (recordSize[0] == -1) {
        // FOUND PLACEHOLDER, LOAD THE REAL RECORD
        return getRecordInternal(new RID(database, rid.getBucketId(), page.readLong((int) (recordPositionInPage + recordSize[1]))), true);
      }

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on lookup of record " + rid, e);
    }
  }

  private RID createRecordInternal(final Record record, final boolean isPlaceHolder) {
    final Binary buffer = database.getSerializer().serialize(database, record);

    if (buffer.size() > pageSize - contentHeaderSize)
      // TODO: SUPPORT MULTI-PAGE CONTENT
      throw new DatabaseOperationException(
          "Record too big to be stored in bucket '" + name + "' (" + id + "), size=" + buffer.size() + " max=" + (pageSize - contentHeaderSize)
              + ". Change the `arcadedb.bucketDefaultPageSize` database settings and try again");

    // RECORD SIZE CANNOT BE < 5 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER
    while (buffer.size() < 5)
      buffer.putByte(buffer.size() - 1, (byte) 0);

    try {
      int newPosition = -1;
      MutablePage lastPage = null;
      int recordCountInPage = -1;
      boolean createNewPage = false;

      final int txPageCounter = getTotalPages();

      if (txPageCounter > 0) {
        lastPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), txPageCounter - 1), pageSize, false);
        recordCountInPage = lastPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        if (recordCountInPage >= maxRecordsInPage)
          // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
          createNewPage = true;
        else if (recordCountInPage > 0) {
          // GET FIRST EMPTY POSITION
          final int lastRecordPositionInPage = (int) lastPage.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (recordCountInPage - 1) * INT_SERIALIZED_SIZE);
          if (lastRecordPositionInPage == 0)
            // CLEANED CORRUPTED RECORD
            createNewPage = true;

          final long[] lastRecordSize = lastPage.readNumberAndSize(lastRecordPositionInPage);

          if (lastRecordSize[0] > 0)
            newPosition = lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
          else if (lastRecordSize[0] == -1)
            newPosition = lastRecordPositionInPage + LONG_SERIALIZED_SIZE + 1;
          else
            newPosition = lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];

          if (newPosition + INT_SERIALIZED_SIZE + buffer.size() > lastPage.getMaxContentSize())
            // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
            createNewPage = true;

        } else
          // FIRST RECORD, START RIGHT AFTER THE HEADER
          newPosition = contentHeaderSize;
      } else
        createNewPage = true;

      if (createNewPage) {
        lastPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);
        //lastPage.blank(0, CONTENT_HEADER_SIZE);
        newPosition = contentHeaderSize;
        recordCountInPage = 0;
      }

      final RID rid = new RID(database, file.getFileId(), ((long) lastPage.getPageId().getPageNumber()) * maxRecordsInPage + recordCountInPage);

      final byte[] array = buffer.toByteArray();

      final int byteWritten = lastPage.writeNumber(newPosition, isPlaceHolder ? (-1L * array.length) : array.length);
      lastPage.writeByteArray(newPosition + byteWritten, array);

      lastPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordCountInPage * INT_SERIALIZED_SIZE, newPosition);

      lastPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) ++recordCountInPage);

      LogManager.instance()
          .log(this, Level.FINE, "Created record %s (page=%s records=%d threadId=%d)", rid, lastPage, recordCountInPage, Thread.currentThread().getId());

      ((RecordInternal) record).setBuffer(buffer.copy());

      return rid;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + name + "'", e);
    }
  }

  private boolean updateRecordInternal(final Record record, final RID rid, final boolean updatePlaceholder) {
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
      if (recordSize[0] == 0)
        // DELETED
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      boolean isPlaceHolder = false;
      if (recordSize[0] < -1) {
        if (!updatePlaceholder)
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        isPlaceHolder = true;
        recordSize[0] *= -1;

      } else if (recordSize[0] == -1) {

        // FOUND A RECORD POINTED FROM A PLACEHOLDER
        final RID placeHolderRID = new RID(database, id, page.readLong((int) (recordPositionInPage + recordSize[1])));
        if (updateRecordInternal(record, placeHolderRID, true))
          return true;

        // DELETE OLD PLACEHOLDER
        deleteRecordInternal(placeHolderRID, true);

        recordSize[0] = LONG_SERIALIZED_SIZE;
        recordSize[1] = 1;
      }

      if (buffer.size() > recordSize[0]) {
        // MAKE ROOM IN THE PAGE IF POSSIBLE

        final int lastRecordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (recordCountInPage - 1) * INT_SERIALIZED_SIZE);
        final long[] lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);

        if (lastRecordSize[0] == -1) {
          lastRecordSize[0] = LONG_SERIALIZED_SIZE;
          lastRecordSize[1] = 1;
        } else if (lastRecordSize[0] < -1) {
          lastRecordSize[0] *= -1;
        }

        final int pageOccupied = (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]);

        final int bufferSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * buffer.size() : buffer.size());

        final int delta = (int) (buffer.size() + bufferSizeLength - recordSize[0] - recordSize[1]);

        if (page.getMaxContentSize() - pageOccupied > delta) {
          // THERE IS SPACE LEFT IN THE PAGE, SHIFT ON THE RIGHT THE EXISTENT RECORDS

          if (positionInPage < recordCountInPage - 1) {
            // NOT LAST RECORD IN PAGE, SHIFT NEXT RECORDS
            final int nextRecordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (positionInPage + 1) * INT_SERIALIZED_SIZE);

            final int newPos = nextRecordPositionInPage + delta;

            page.move(nextRecordPositionInPage, newPos, pageOccupied - nextRecordPositionInPage);

            // TODO: CALCULATE THE REAL SIZE TO COMPACT DELETED RECORDS/PLACEHOLDERS
            for (int pos = positionInPage + 1; pos < recordCountInPage; ++pos) {
              final int nextRecordPosInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE);

              if (nextRecordPosInPage == 0)
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, 0);
              else
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, nextRecordPosInPage + delta);

              assert nextRecordPosInPage + delta < page.getMaxContentSize();
            }
          }

          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * buffer.size() : buffer.size());
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

          page.writeByteArray(recordContentPositionInPage, buffer.toByteArray());

          LogManager.instance().log(this, Level.FINE, "Updated record %s by allocating new space on the same page (page=%s threadId=%d)", null, rid, page,
              Thread.currentThread().getId());

        } else {
          if (isPlaceHolder)
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            return false;

          // STORE THE RECORD SOMEWHERE ELSE AND CREATE HERE A PLACEHOLDER THAT POINTS TO THE NEW POSITION. IN THIS WAY THE RID IS PRESERVED
          final RID realRID = createRecordInternal(record, true);

          final int bytesWritten = page.writeNumber(recordPositionInPage, -1);
          page.writeLong(recordPositionInPage + bytesWritten, realRID.getPosition());
          LogManager.instance().log(this, Level.FINE, "Updated record %s by allocating new space with a placeholder (page=%s threadId=%d)", null, rid, page,
              Thread.currentThread().getId());
        }
      } else {

        recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * buffer.size() : buffer.size());
        final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
        page.writeByteArray(recordContentPositionInPage, buffer.toByteArray());

        LogManager.instance().log(this, Level.FINE, "Updated record %s with the same size or less as before (page=%s threadId=%d)", null, rid, page,
            Thread.currentThread().getId());

      }

      ((RecordInternal) record).setBuffer(buffer.copy());

      return true;

    } catch (IOException e) {
      throw new DatabaseOperationException("Error on update record " + rid);
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholder) {
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
      if (recordPositionInPage == 0)
        // CLEANED CORRUPTED RECORD
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      // AVOID DELETION OF CONTENT IN CORRUPTED RECORD
      if (recordPositionInPage > 0 && recordPositionInPage < page.getContentSize()) {
        final long[] removedRecordSize = page.readNumberAndSize(recordPositionInPage);
        if (removedRecordSize[0] == 0)
          // ALREADY DELETED
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        if (removedRecordSize[0] < -1) {
          if (!deletePlaceholder)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
          removedRecordSize[0] *= -1;
        }

        // CONTENT SIZE = 0 MEANS DELETED
        page.writeNumber(recordPositionInPage, 0);

      } else {
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
      }

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
      throw new DatabaseOperationException("Error on deletion of record " + rid);
    }
  }
}
