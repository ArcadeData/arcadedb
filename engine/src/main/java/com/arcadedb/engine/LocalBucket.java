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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.PageCorruptionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntIntHashMap;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.LongHashSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;

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
 * <li>-4 = first chunk of a placeholder content record that no page could host whole</li>
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
  // Bucket file-format version 1 is reserved for paired external-property buckets. They hold heavier records,
  // so the page-slot table is sized down to 256 (vs 2048 at v0). The bucket-page header (computed by
  // contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + maxSlots*4) shrinks from 8194 bytes at v0 to 1026 bytes at
  // v1 - reclaiming ~7KB of header per page that becomes available for value blobs. Sized to host typical
  // 1-2KB records (especially with compression enabled).
  public static final    int                       EXTERNAL_BUCKET_VERSION          = 1;
  private static final   int                       DEF_MAX_RECORDS_IN_PAGE_V1       = 256;
  public static final    long                      RECORD_PLACEHOLDER_POINTER       = -1L;    // USE -1 AS SIZE TO STORE A PLACEHOLDER (THAT POINTS TO A RECORD ON ANOTHER PAGE)
  public static final    long                      FIRST_CHUNK                      = -2L;    // USE -2 TO MARK THE FIRST CHUNK OF A BIG RECORD. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK
  public static final    long                      NEXT_CHUNK                       = -3L;    // USE -3 TO MARK THE SECOND AND FURTHER CHUNK THAT IS PART OF A BIG RECORD THAT DOES NOT FIT A PAGE. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK OR 0 IF THE CURRENT CHUNK IS THE LAST (NO FURTHER CHUNKS)
  /**
   * The head chunk of a placeholder CONTENT record - same on-page layout as {@link #FIRST_CHUNK}, and reached through a
   * {@link #RECORD_PLACEHOLDER_POINTER} on another page instead of standing for a record of its own (#6196).
   * <p>
   * A content record is otherwise recognised by the NEGATIVE size marker its slot carries, and every reader that walks
   * a page skips it on that basis. A content record no page can host whole has no size in its slot at all - it is a
   * chunk chain - so before this marker existed its head was written as a plain {@link #FIRST_CHUNK} and the "this slot
   * is somebody's content" information was lost at that moment: {@code scan()} handed the bytes out a second time as a
   * document of their own, {@code count()} counted them, and {@code check()} reported a multi-page record where there
   * was a surrogate.
   * <p>
   * It costs nothing in the stored format: -4 was the one value the marker namespace still had free between
   * {@link #NEXT_CHUNK} and {@link #RECORD_PLACEHOLDER_CONTENT}, and it is one zigzag byte exactly like the markers
   * either side of it. Databases written before it still hold the ambiguous shape; {@link #check} reports one and
   * repairs it in place with {@code fix}, so no reader has to tolerate the ambiguity for ever.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  public static final    long                      FIRST_CHUNK_PLACEHOLDER_CONTENT  = -4L;
  protected static final int                       PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int                       PAGE_RECORD_TABLE_OFFSET         =
          PAGE_RECORD_COUNT_IN_PAGE_OFFSET + Binary.SHORT_SERIALIZED_SIZE;
  private static final   int                       DEF_MAX_RECORDS_IN_PAGE          = 2048;
  /**
   * Every record is padded to at least this many bytes of content, because the size a slot stores doubles as the
   * marker namespace: a placeholder CONTENT record stores its size NEGATED, so a record shorter than 5 bytes would
   * write a -1..-4 that {@link #RECORD_PLACEHOLDER_POINTER} (-1), {@link #FIRST_CHUNK} (-2) and
   * {@link #NEXT_CHUNK} (-3) already own. Hence {@link #RECORD_PLACEHOLDER_CONTENT} = -5: anything below it is a
   * size, anything above it is a marker.
   * <p>
   * It says nothing about how much room a record needs to SPILL - that is
   * {@link #MINIMUM_SPACE_FOR_FIRST_CHUNK}, and the two are independent. (The comment that used to sit here
   * conflated them, claiming a record "cannot be < 13 bytes in case of update and placeholder".)
   */
  private static final   int                       MINIMUM_RECORD_SIZE              = 5;
  /** Boundary between the size of a placeholder content record (stored negated, so &lt; -5) and the markers above it. */
  private static final   long                      RECORD_PLACEHOLDER_CONTENT       = MINIMUM_RECORD_SIZE * -1L;
  /**
   * Bytes a chunk's marker is BUDGETED at when room for the chunk is being asked for, as opposed to the bytes it
   * really took, which {@code writeNumber} reports once the marker is written. Two rather than one because the budget
   * has to hold for every marker a chunk can wear, and over-budgeting by a byte only ever leaves a chunk one byte of
   * content short - it can never make a request smaller than the write that follows it.
   */
  private static final   int                       CHUNK_MARKER_BUDGET              = 2;
  /**
   * Bytes a slot needs to host the HEAD chunk of a multi-page record: the {@link #FIRST_CHUNK} marker (at
   * {@link #CHUNK_MARKER_BUDGET}), the chunk size and the pointer to the next chunk, plus at least one byte of
   * content. That is {@link #chunkFootprint} of an empty chunk, which is what makes this the same arithmetic every
   * writer and every page walk uses. A slot that cannot reach it is the only case left where a record that outgrows
   * its page still spills into a placeholder instead of a chunk chain (#6149).
   * <p>
   * <b>The 2 budgeted for the marker is deliberately more than the 1 byte {@code writeNumber} really spends on
   * {@link #FIRST_CHUNK}, and that does not put this constant at odds with the {@code Binary.getNumberSpace} the
   * replay measures.</b> Both sides target the same TOTAL footprint, never the marker on its own:
   * {@code writeMultiPageRecord} derives {@code chunkSize} from the room it is given minus the marker it actually
   * wrote, so the spare byte simply becomes chunk content ({@code 14 - 1 - 4 - 8 = 1} byte of content), and
   * {@code rebaseRecordOnPage} re-derives the same 14 as {@code getNumberSpace(FIRST_CHUNK) + body.length}, the
   * body being the chunk image those very bytes produced. Raising the budget only makes the head chunk carry one
   * more byte of content; it can never make the two sides disagree.
   */
  private static final   int                       MINIMUM_SPACE_FOR_FIRST_CHUNK    = chunkFootprint(CHUNK_MARKER_BUDGET, 0);
  /**
   * No fingerprint could be taken of the part of a record that lives off its own page: see
   * {@link #offPageContentFingerprint(RID, BasePage, boolean)}.
   */
  public static final    long                      NO_OFF_PAGE_FINGERPRINT          = 0L;
  /**
   * The record has nothing off its own page - a chunk chain of one chunk, all of it in the head. A real value,
   * distinct from "unknown", and also where a hash that lands on {@link #NO_OFF_PAGE_FINGERPRINT} is moved to.
   */
  private static final   long                      EMPTY_OFF_PAGE_FINGERPRINT       = 1L;
  private static final   long                      FNV_OFFSET_BASIS                 = 0xcbf29ce484222325L;
  private static final   long                      FNV_PRIME                        = 0x100000001b3L;
  private static final   int                       CHUNKS_BEFORE_LOOP_DETECTION     = 64;
  /**
   * Layout of the trace {@link #loadMultiPageRecord} keeps of the chain it walked, one stride per chunk consumed, so
   * the read can be validated against what it actually READ instead of against the version of the pages that read
   * happened to touch (#6217). Four longs in one flat array rather than an object per chunk: this is the read path of
   * every record too big for its page, and the trace is thrown away at the end of the read.
   * <p>
   * The two things NOT in it are the ones that are derivable: a chunk's content offset inside the assembled record is
   * the sum of the sizes before it, and the pointer a chunk carries to the next one is that chunk's own
   * (page, slot) - which is why the validation can check the chain's SHAPE, and not only its bytes, without storing
   * anything more.
   */
  private static final   int                       CHAIN_TRACE_STRIDE               = 4;
  private static final   int                       CHAIN_TRACE_PAGE_NUMBER          = 0;
  private static final   int                       CHAIN_TRACE_PAGE_VERSION         = 1;
  private static final   int                       CHAIN_TRACE_SLOT                 = 2;
  private static final   int                       CHAIN_TRACE_CHUNK_SIZE           = 3;
  /** No page the read walked has moved since: the chain is trivially the one that was read. */
  private static final   int                       CHAIN_READ_UNCHANGED             = 0;
  /** A page the read walked has moved, but not one byte this record owns on it: the read stands (#6217). */
  private static final   int                       CHAIN_READ_REVALIDATED           = 1;
  /** The record itself moved under the read: what was assembled may be a mix of two commits, so it is thrown away. */
  private static final   int                       CHAIN_READ_CHANGED               = 2;
  private static final   long                      MINIMUM_SPACE_LEFT_IN_PAGE       = 50L;
  private static final   int                       MAX_PAGES_GATHER_STATS           = 100;
  private static final   long                      MAX_TIMEOUT_GATHER_STATS         = 5000L;
  private static final   int                       GATHER_STATS_MIN_SPACE_PERC      = 10;
  /**
   * Whether the free-space claims the write paths make are held against the commit-time measurement (#6396). On
   * exactly when assertions are - surefire's default, never a production run - because the check IS an assertion:
   * it costs two field stores per write, and a comparison against a page walk the compression was doing anyway.
   * <p>
   * It is gated a second time, by {@code reuseSpaceMode}: {@link #updatePageStatistics} returns before stating a
   * claim below {@link REUSE_SPACE_MODE#HIGH}, so under a non-default {@code arcadedb.bucketReuseSpaceMode} every
   * page stays at {@link MutablePage#FREE_SPACE_CLAIM_UNKNOWN} and the check goes quiet. That is right rather than
   * an oversight - there are no deltas to keep honest when nothing is tracking free space - but it is worth knowing
   * before wondering why the assertion never fires.
   */
  private static final   boolean                   CHECK_FREE_SPACE_CLAIMS          = LocalBucket.class.desiredAssertionStatus();
  private static final   int                       SPARE_SPACE_FOR_GROWTH           = 32;
  protected final        int                       contentHeaderSize;
  private final          int                       maxRecordsInPage;
  private final          AtomicLong                cachedRecordCount                = new AtomicLong(-1);

  /**
   * Bucket purpose tag. Declared up here (next to the {@link #purpose} field that uses it) so the enum is the
   * first thing a reader sees alongside the other bucket-level constants - the alternative was burying it
   * mid-file between unrelated state, which made the EXTERNAL property contract hard to discover.
   */
  public enum Purpose {
    /** Bucket holding the primary records of a type (vertex/edge/document). Targetable by user DML. */
    PRIMARY,
    /** Paired infrastructure bucket holding externalised property values. NOT targetable by user DML. */
    EXTERNAL_PROPERTY
  }

  // Buckets are PRIMARY by default (they hold the primary records of a type and are user-targetable via DML).
  // Internal kinds (e.g. EXTERNAL_PROPERTY) hold serializer infrastructure that user-facing DML must not target.
  // The purpose is persisted in schema.json (per-type) and restored at load time, see LocalDocumentType.
  private                Purpose                   purpose                          = Purpose.PRIMARY;
  // pageId → free-space-bytes. TreeMap ordering is unused (verified by grep), so a primitive
  // open-addressing map saves memory and avoids Integer boxing on every read/write/remove on
  // the page-allocation hot path. Bounded by MAX_PAGES_GATHER_STATS (100). Single-threaded
  // access is enforced by `synchronized (freeSpaceInPages)` blocks at every callsite.
  private final          IntIntHashMap             freeSpaceInPages                 = new IntIntHashMap();
  // #5279: pageNumber → slots/space handed out to the transactions that are currently inserting into that page but
  // have not committed yet. Without it, every concurrent transaction inserting into the same bucket picked the SAME
  // free slot of the SAME page (same optimistic RID) and all but the first failed at commit with a page-level
  // ConcurrentModificationException, even though two inserts of DIFFERENT records always commute. An entry exists
  // only while at least one transaction is inserting into that page, so the map is as small as the write
  // concurrency; see reserveInsertSlot().
  private final          Map<Integer, PageInsertReservation> insertReservations     = new ConcurrentHashMap<>();
  private final          REUSE_SPACE_MODE          reuseSpaceMode;
  // #4958: both fields are read/written outside the freeSpaceInPages monitor on some paths (delete,
  // updatePageStatistics), so they must be safe on their own: volatile timestamp + atomic counter.
  private volatile       long                      timeOfLastStats                  = 0L;
  private final          AtomicLong                changesFromLastStats             = new AtomicLong();

  private enum REUSE_SPACE_MODE {
    LOW, MEDIUM, HIGH
  }

  /**
   * Slots and page space handed out to the transactions currently inserting into ONE bucket page (issue #5279).
   * Every access is made under the entry's own monitor, which serialises the claims on a given page.
   */
  private static final class PageInsertReservation {
    /** First slot index no transaction has been given yet: the append high-water mark of the page. */
    private int          nextFreeSlot;
    /** First content offset no transaction has been given yet: the space high-water mark of the page. */
    private int          nextFreeOffset;
    /** Slots reserved by still-running transactions. */
    private final BitSet reservedSlots = new BitSet();
    /** Number of reservations not released yet: the whole entry is dropped when it reaches zero. */
    private int          outstanding;
    /**
     * The only transaction holding reservations on this page, or {@code null} once a second one joined. While a page
     * is uncontended its owner's own view of it is authoritative and the high-water marks must NOT be used: they only
     * ever grow, so a long transaction that deletes and re-inserts (a bulk rewrite) would otherwise see the page as
     * permanently full and start a new one per record.
     */
    private volatile Object soleOwner;
    /** Set when the entry was taken out of the map, so a claim that raced with the removal retries on a fresh one. */
    private boolean         removed;
  }

  private static final Function<Integer, PageInsertReservation> NEW_INSERT_RESERVATION = k -> new PageInsertReservation();

  /**
   * What a slot {@link #check} walked turned out to hold. Decided while the slot is read and counted ONCE at the end
   * of the block (#6293): the classification used to increment the counters inline while the repairs ran after it, so
   * a record CHECK DATABASE FIX deleted during the run was reported both under {@code totalDeletedRecords} and under
   * the category it had held before - one run describing the same record twice, which is exactly what an operator
   * diffs across runs to decide whether a FIX did anything.
   */
  private enum SlotCategory {
    /** Counted by nothing: a slot too corrupted to classify that this run was not asked to repair. */
    UNCLASSIFIED,
    DELETED,
    ACTIVE,
    PLACEHOLDER_POINTER,
    SURROGATE,
    MULTI_PAGE,
    CHUNK
  }

  /**
   * What the target of a placeholder POINTER turned out to be (see {@link #classifyPlaceholderTarget}).
   */
  private enum PlaceholderTarget {
    /** A content record of a shape this build recognises: a negated size marker, or a head chunk that says so. */
    CONTENT,
    /**
     * A head chunk still wearing the ambiguous {@link #FIRST_CHUNK} - what every release before #6196 wrote for a
     * content record no page could host whole, and what a scan hands out a second time as a document of its own.
     */
    LEGACY_AMBIGUOUS,
    /**
     * Nothing a pointer may lead to: a slot that was deleted, a position past the end of the bucket, or a marker that
     * is neither a negated size nor a content head. The record is then gone from every scan and still counted by
     * {@code count(*)}, which is the disagreement #6292 is about.
     */
    DANGLING,
    /** The target's page could not be read: an I/O fault, and evidence of nothing. */
    UNREADABLE
  }

  /**
   * The tallies {@link #check} accumulates. One object rather than a dozen locals so the passes that RECONCILE them
   * after the walk - the placeholder pointers (#6292, #6196) and the orphaned chunks (#6294) - can be named methods
   * instead of another few hundred lines inline.
   */
  private static final class CheckStats {
    private long totalAllocatedRecords;
    private long totalActiveRecords;
    private long totalPlaceholderRecords;
    private long totalSurrogateRecords;
    private long totalMultiPageRecords;
    private long totalDeletedRecords;
    private long totalMaxOffset;
    private long totalChunks;
    private long orphanedChunks;
    private long orphanedChunksReclaimed;
    private long danglingPlaceholderPointers;
    private long danglingPlaceholderPointersFixed;
    private long totalErrors;

    private final List<String> warnings               = new ArrayList<>();
    private final List<RID>    deletedRecordsAfterFix = new ArrayList<>();
  }

  /**
   * The continuation chunks a {@link #findBrokenChunkChain} walk went through, and whether it reached a conclusion.
   * <p>
   * The set is the walk's own loop detector - revisiting a continuation pointer is the only certain loop signal - so
   * a caller that needs to know WHICH chunks a record reaches (#6294) is asking the question the walk already answers,
   * and is handed the answer rather than costing a second walk.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final class ChunkChainWalk {
    private final LongHashSet chunks = new LongHashSet();
    /**
     * The walk stopped without proving anything: a page it could not LOAD, which is an I/O fault and not a broken
     * chain (see {@link #findBrokenChunkChain}). The chunks past that point are neither reached nor known to be
     * unreachable, so a caller reasoning about reachability has to fail closed on it.
     */
    private boolean incomplete;
    /**
     * Where the walk broke: the index of the chunk whose continuation pointer could not be resolved, and the value
     * of that pointer. {@code -1}/{@code 0} while the walk has proved nothing.
     * <p>
     * This is what lets a SECOND walk be compared against a first one rather than merely agreed with (#6282): the
     * delete path meets a break in the transaction's view and asks the newest committed image whether the same hop,
     * to the same target, still fails. A committed image that breaks somewhere else is a record that MOVED under the
     * delete, which is contention and must keep its retry - not the permanent corruption verdict.
     * <p>
     * Set exactly when {@link #findBrokenChunkChain} returns a reason, and back to {@code -1}/{@code 0} on every exit
     * that proves nothing - the clean walk included.
     */
    private int  brokenAtChunk = -1;
    private long brokenAtPointer;

    private void reset() {
      chunks.clear();
      incomplete = false;
      brokenAtChunk = -1;
      brokenAtPointer = 0;
    }
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
   * Record position (the RID's long position) for a slot in a page. Package-private static so the overflow
   * regression test for #4931 can verify it directly: the pre-fix inline {@code int * int} arithmetic
   * overflowed for buckets beyond 2^31 positions, and {@code check(fix=true)} then deleted an innocent
   * record at the wrong RID.
   */
  static long recordPosition(final int pageId, final int maxRecordsInPage, final int positionInPage) {
    return (long) pageId * maxRecordsInPage + positionInPage;
  }

  /**
   * Called at creation time.
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
                     final int pageSize, final int version) throws IOException {
    super(database, name, filePath, BUCKET_EXT, mode, pageSize, version);
    this.maxRecordsInPage = maxRecordsInPageForVersion(version);
    this.contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.cachedRecordCount.set(0);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
    this.purpose = purposeForVersion(version);
  }

  /**
   * Called at load time.
   * <p>
   * Free-space statistics are NOT pre-warmed here. {@link #findAvailableSpace} already calls
   * {@link #gatherPageStatistics()} lazily on the first allocation that needs it, so a leader
   * still gets reuse for free; a follower that only applies leader-shipped pages via the state
   * machine never triggers it at all. Pre-warming here would scan up to all pages of every
   * bucket during {@link com.arcadedb.schema.LocalSchema#load} - which on a follower under
   * heavy bulk-load fires repeatedly per LSM compaction SCHEMA_ENTRY and exhausts the heap
   * (issue #4219).
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
                     final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.maxRecordsInPage = maxRecordsInPageForVersion(version);
    contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
    // Derive purpose from the bucket file version - the version itself is persisted in the on-disk file name
    // (e.g. Doc_0_ext.<id>.<pageSize>.v1.bucket), so this assignment is reliable as soon as the LocalBucket
    // is constructed - long before LocalDocumentType.restoreExternalBuckets() runs. That closes the gap where
    // a write path firing between FileManager scan and schema-load completion would have seen purpose=PRIMARY
    // by default and bypassed the user-DML guard. Schema JSON still maps primary->external by name (which is
    // an orthogonal concern), but the write guard now no longer depends on schema-load ordering.
    this.purpose = purposeForVersion(version);
  }

  private static Purpose purposeForVersion(final int version) {
    return version >= EXTERNAL_BUCKET_VERSION ? Purpose.EXTERNAL_PROPERTY : Purpose.PRIMARY;
  }

  @Override
  public void close() {
    super.close();
    freeSpaceInPages.clear();
    insertReservations.clear();
  }

  public int getMaxRecordsInPage() {
    return maxRecordsInPage;
  }

  /** Slot-table sizing for the bucket file format version. v0=2048 (legacy), v1=256 (paired external buckets). */
  private static int maxRecordsInPageForVersion(final int version) {
    return version >= EXTERNAL_BUCKET_VERSION ? DEF_MAX_RECORDS_IN_PAGE_V1 : DEF_MAX_RECORDS_IN_PAGE;
  }

  public Purpose getPurpose() {
    return purpose;
  }

  public void setPurpose(final Purpose purpose) {
    this.purpose = purpose;
  }

  @Override
  public RID createRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    // Provisional identity for Document records (and subtypes: vertex, edge) so the serializer can resolve the
    // target primary bucket for EXTERNAL property handling. EdgeSegment, ExternalValueRecord, and other internal
    // record types do not need this and must not be touched.
    if (record.getIdentity() == null && record instanceof Document && record instanceof RecordInternal ri)
      ri.setIdentity(RID.create(database, fileId, -1L));
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

    try {
      // #6196: FIRST_CHUNK and not isChunkHead(), deliberately. A record EXISTS at this RID when the slot holds one of
      // its own; the head chunk of a placeholder's CONTENT holds somebody else's bytes and answers false here exactly
      // as the negated size marker of a content record that fitted its page always has.
      final long[] recordSize = readRecordSizeMarker(rid);
      return recordSize != null && (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER
          || recordSize[0] == FIRST_CHUNK);

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  /**
   * Tells whether the record is stored entirely in place on its own page (positive size marker), as opposed to
   * a placeholder indirection or a multi-page record whose continuation chunks live on other pages. The
   * commit-time edge-append rebase ({@code TransactionContext.rebaseEdgeAppends}) relies on this: rebasing
   * re-reads and re-writes a record assuming its bytes live on the ONE conflicted page, so any record with
   * content on other pages must fall back to a full-transaction retry - re-writing it would publish the
   * transaction's stale copy of the continuation pages, silently reverting concurrently committed bytes there
   * (those pages pass the MVCC version check because their transaction copy is created only at drain time).
   * Reads the CURRENT state through the transaction (the caller holds the file's commit lock).
   */
  public boolean isRecordStoredInSinglePage(final RID rid) {
    if (rid == null || rid.getPosition() < 0)
      return false;

    try {
      final long[] recordSize = readRecordSizeMarker(rid);
      return recordSize != null && recordSize[0] > 0;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on checking record layout for " + rid, e);
    }
  }

  /**
   * Reads the raw size marker of the record's slot on its page through the transaction (positive = in-place
   * record, {@link #RECORD_PLACEHOLDER_POINTER}, {@link #FIRST_CHUNK}, {@link #NEXT_CHUNK}, negative =
   * placeholder content), or {@code null} when the page or slot does not exist or the record was deleted.
   */
  private long[] readRecordSizeMarker(final RID rid) throws IOException {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        return null;
    }

    final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);

    final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    if (positionInPage >= recordCountInPage)
      return null;

    final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
    if (recordPositionInPage == 0)
      // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
      return null;

    return page.readNumberAndSize(recordPositionInPage);
  }

  /**
   * Whether the size marker of a slot says it holds the HEAD chunk of a multi-page record - a record of its own
   * ({@link #FIRST_CHUNK}) or the CONTENT of a placeholder ({@link #FIRST_CHUNK_PLACEHOLDER_CONTENT}, #6196). The two
   * carry the same header and the same chain behind it, so every reader that walks or rewrites the CHAIN asks this,
   * and only the readers that decide whether the slot is a RECORD tell them apart.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static boolean isChunkHead(final long recordSizeMarker) {
    return recordSizeMarker == FIRST_CHUNK || recordSizeMarker == FIRST_CHUNK_PLACEHOLDER_CONTENT;
  }

  /**
   * Whether the size marker of a slot says it holds the CONTENT of a placeholder, whichever of the two shapes it is
   * stored in: a negated size when a page could host it whole, or the head of a chunk chain when none could (#6196).
   * Such a slot is not a record - it is reachable only through the {@link #RECORD_PLACEHOLDER_POINTER} that references
   * it - which is what every scan, count and existence check has to know about it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static boolean isPlaceholderContent(final long recordSizeMarker) {
    return recordSizeMarker < RECORD_PLACEHOLDER_CONTENT || recordSizeMarker == FIRST_CHUNK_PLACEHOLDER_CONTENT;
  }

  @Override
  public void deleteRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false, false);
  }

  /**
   * Force-deletes a record even when its multi-page chunk chain is structurally broken. A normal delete walks the
   * chain to free every chunk and, on a broken link, throws {@link ConcurrentModificationException} as a retry
   * signal (#4932) so it never orphans chunks. That guard makes a genuinely-corrupt record undeletable by every
   * path. With {@code force=true} a broken link stops the chain walk instead of aborting: the head slot is still
   * freed so the record finally disappears, and any chunks past the break are orphaned - a bounded space leak that
   * {@code CHECK DATABASE FIX} reclaims, by finding the chunks no chain reaches (#6294). Compaction does not and
   * never did: {@code compressPage} re-flows a page's LIVE slots, and an orphaned chunk still has one.
   * Intended for admin repair (CHECK DATABASE FIX), not the hot path.
   */
  public void deleteRecord(final RID rid, final boolean force) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false, force);
  }

  /**
   * Deletes a record an integrity check has already judged beyond repair, escalating to {@link #deleteRecord(RID,
   * boolean) force} only for the one failure that force exists to clear: a structurally broken chunk chain.
   * <p>
   * The escalation is GATED, not unconditional, and that is the whole point of this method. Since #6282 the delete
   * itself names a break it has confirmed against the newest committed image, so the common case needs no second
   * question at all. What is left under {@link ConcurrentModificationException} does: it does not prove corruption -
   * the same page-version validation fails when concurrent writes touch OTHER records sharing the chain's pages, so
   * on a busy bucket it is ordinary contention, and forcing through it would orphan chunks and skip index cleanup
   * for a healthy record. The version-blind structural probe is what tells the two apart; it is the same
   * discriminator {@code LocalDatabase.deleteRecord} uses for the tolerant path, and a transient conflict still
   * propagates as the retry signal it is.
   * <p>
   * Deliberately does NOT consult {@code DELETE_TOLERATE_BROKEN_CHAIN}: that setting governs whether an ORDINARY
   * delete may force through, and its own documentation states that CHECK DATABASE FIX is unaffected by it either
   * way, so a broken record is never permanently stuck. {@link #check} already force-deletes such a record on the
   * bucket-wide pass; this is the same guarantee for the callers that reach one record directly, which is what the
   * RECORD-scoped check does after skipping the bucket-wide passes.
   * <p>
   * Note for the caller: like both {@code deleteRecord} overloads, this does NOT maintain {@code cachedRecordCount}
   * - the counter {@code count(*)} answers from. Pair it with {@code updateBucketRecordDelta(fileId, -1)}.
   */
  public void deleteCorruptedRecord(final RID rid) {
    try {
      deleteRecord(rid);
    } catch (final BrokenChunkChainException e) {
      // NO PROBE NEEDED: the delete's own walk confirmed the break against the newest committed image before saying
      // so (#6282). This is the case force exists for, and the one this method escalates for.
      LogManager.instance().log(this, Level.FINE, "Force-deleting record %s: %s", e, rid, e.getMessage());
      deleteRecord(rid, true);
    } catch (final ConcurrentModificationException e) {
      // CONTENTION, or a break the confirmation could not prove. The probe is what tells them apart, and since #6282
      // it too reads the newest committed image rather than this transaction's pinned view - which matters here more
      // than anywhere, because a false positive escalates to a FORCE delete of a healthy record.
      if (!isChunkChainBroken(rid))
        // CONTENTION, not corruption: preserve the NeedRetryException semantics so the caller can retry.
        throw e;
      deleteRecord(rid, true);
    }
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
            final RID rid = new RID(fileId, recordPosition(pageId, maxRecordsInPage, recordIdInPage));

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
                final RID placeHolderPointer = new RID(fileId,
                        page.readLong((int) (recordPositionInPage + recordSize[1])));
                final Binary view = getRecordInternal(placeHolderPointer, true);
                if (view != null && !callback.onRecord(rid, view))
                  return;
              } else if (recordSize[0] == FIRST_CHUNK) {
                // LOAD THE ENTIRE RECORD IN CHUNKS. #6196: FIRST_CHUNK and not isChunkHead() - a scan hands out
                // records, and the head chunk of a placeholder's CONTENT is not one. Its bytes reach the caller
                // through the pointer branch above, under the RID the user knows, and exactly once.
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

  /**
   * Fingerprint of the part of the record {@code rid} that does NOT live in its own slot, or
   * {@link #NO_OFF_PAGE_FINGERPRINT} when there is no such part (an ordinary record) or it cannot be read.
   * <p>
   * A record update is deferred: {@code TransactionContext.addUpdatedRecord} pins the record's own page at
   * {@code save()} time and the write runs at commit, after the file's commit lock is taken - so every OTHER page the
   * write touches is loaded fresh, at the newest committed version, and can never fail a page-version check. Whatever
   * a record keeps outside its own slot is therefore invisible to MVCC, and this is what makes it visible again:
   * taken twice, once when the record is taken for update and once at commit before the write, the two values are
   * equal exactly when no concurrent transaction has touched that part of the record meanwhile.
   * <p>
   * Two record shapes have such a part, and both are answered here because both are taken at the same moment, for the
   * same reason:
   * <ul>
   * <li>a multi-page record's chunk chain PAST its head chunk (#6129). The head chunk is the only part on the page
   * the disjoint-slot merge replays, so the byte-for-byte pre-image check it makes sees that and nothing else; a
   * mismatch here keeps the page on the ordinary retry path it took before that merge existed. Only walked when the
   * merge is on ({@code withChunkChainTail}): with it off the head chunk's page is poisoned unconditionally and a
   * concurrent write to the record fails the version check on it anyway, so the walk would buy nothing.</li>
   * <li>the CONTENT record behind a placeholder POINTER (#6141). Here the slot holds 8 bytes of pointer and the
   * record's WHOLE content is off-page, so nothing at all was version-checked: two transactions updating such a
   * record both loaded the content page fresh under the commit lock and the second one silently won. A mismatch is a
   * genuine concurrent modification and the update path raises a retryable
   * {@link com.arcadedb.exception.ConcurrentModificationException} on it. Always taken - unlike the chain tail, it is
   * the only thing standing between that record and a lost update.</li>
   * </ul>
   * <p>
   * {@code recordPage} is the page the record lives on, which every caller has just pinned
   * ({@link #fetchPageInTransaction(RID)} returns it): taking it as a parameter keeps this off the update path's
   * allocation budget, since resolving it again would cost a {@link PageId} per updated record.
   * <p>
   * It reads through the transaction, so a page this transaction has already rewritten is fingerprinted as this
   * transaction sees it, and the chain walk never allocates per chunk: one page-sized scratch buffer is reused for
   * the whole chain.
   * <p>
   * What goes into it is (RID, size, content) for every chunk of the tail or for the content record, which makes the
   * identity it establishes stronger than "the same bytes": two states with the same fingerprint hold byte-identical
   * data at the same slots. That is what makes an ABA - a chunk or a content record freed and its slot reused
   * meanwhile - a non-event: a concurrent transaction that ended up reproducing this exact image reproduced this
   * exact record content, so there is no write of theirs left to drop.
   * <p>
   * A 64-bit FNV-1a, not a cryptographic digest. An ACCIDENTAL collision has a ~2^-64 chance of costing a lost update
   * on a record two transactions are rewriting at the same time. A DELIBERATE one buys its author nothing: both
   * colliding images are content of the same record, so engineering one requires write access to that record - and
   * whoever has it can already overwrite it by committing last, which is the very outcome the collision would
   * produce. There is no third party whose data a collision could reach.
   *
   * @param withChunkChainTail whether a multi-page record's chain has to be walked, i.e. whether the disjoint-slot
   *                           merge that consumes it is enabled for this transaction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  public long offPageContentFingerprint(final RID rid, final BasePage recordPage, final boolean withChunkChainTail) {
    try {
      if (recordPage == null)
        return NO_OFF_PAGE_FINGERPRINT;

      final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);
      final int recordPositionInPage = getRecordPositionInPage(recordPage, positionInPage);
      if (recordPositionInPage == 0)
        return NO_OFF_PAGE_FINGERPRINT;

      final long[] recordSize = recordPage.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER)
        return placeholderContentFingerprint(
                new RID(fileId, recordPage.readLong((int) (recordPositionInPage + recordSize[1]))));

      if (!isChunkHead(recordSize[0]) || !withChunkChainTail)
        // An ordinary record: the whole of it is in its own slot, nothing left to cover.
        return NO_OFF_PAGE_FINGERPRINT;

      return chunkChainTailFingerprint(recordPage, (int) (recordPositionInPage + recordSize[1]));
    } catch (final Exception e) {
      // A record whose off-page part cannot be read (a page that is gone, a broken pointer) cannot be vouched for.
      return unwalkableChunkChain(e, rid);
    }
  }

  /**
   * Fingerprint of the CONTENT record a placeholder pointer references, {@code contentRID} included so that a content
   * record relocated onto another slot is a different image even when every byte of it is the same (#6141).
   * <p>
   * Never throws, and returns {@link #NO_OFF_PAGE_FINGERPRINT} for anything that is not a readable placeholder
   * content record. The two sides of the comparison read that differently, on purpose: at capture time it means
   * "there is nothing here to vouch for" and the update simply goes unchecked, exactly as it did before this existed;
   * at commit time it can only mean that what WAS a content record no longer is one, which is the concurrent
   * modification the caller is looking for.
   * <p>
   * A content record that is itself a chunk chain is covered too: {@code createRecordInternal} spills one into chunks
   * like any other record when no page can host it whole, and the chain then belongs to the image just as much as the
   * head does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private long placeholderContentFingerprint(final RID contentRID) {
    try {
      final int contentPageId = (int) (contentRID.getPosition() / maxRecordsInPage);
      if (contentPageId >= getTotalPages())
        return NO_OFF_PAGE_FINGERPRINT;

      final BasePage contentPage = database.getTransaction()
              .getPage(new PageId(database, file.getFileId(), contentPageId), pageSize);
      if (contentPage == null)
        return NO_OFF_PAGE_FINGERPRINT;

      final int positionInPage = (int) (contentRID.getPosition() % maxRecordsInPage);
      if (positionInPage >= contentPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return NO_OFF_PAGE_FINGERPRINT;
      final int recordPositionInPage = getRecordPositionInPage(contentPage, positionInPage);
      if (recordPositionInPage == 0)
        return NO_OFF_PAGE_FINGERPRINT;

      final long[] recordSize = contentPage.readNumberAndSize(recordPositionInPage);

      long fingerprint = fnv1a(FNV_OFFSET_BASIS, contentRID.getPosition());
      fingerprint = fnv1a(fingerprint, recordSize[0]);

      final int contentOffset;
      final int contentSize;
      if (isChunkHead(recordSize[0])) {
        // The content record outgrew its own page in turn: its head chunk here, the rest through the chain below.
        // #6196 gave that head a marker of its own, and a database written before it holds the ambiguous FIRST_CHUNK:
        // both are accepted, so a fingerprint taken before a CHECK DATABASE FIX still describes the same record after.
        final int chunkHeaderPos = (int) (recordPositionInPage + recordSize[1]);
        contentSize = contentPage.readInt(chunkHeaderPos);
        contentOffset = chunkHeaderPos + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
        final long tail = chunkChainTailFingerprint(contentPage, chunkHeaderPos);
        if (tail == NO_OFF_PAGE_FINGERPRINT)
          return NO_OFF_PAGE_FINGERPRINT;
        fingerprint = fnv1a(fingerprint, tail);
      } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        // A placeholder content record stores its size NEGATED.
        contentSize = (int) (recordSize[0] * -1L);
        contentOffset = (int) (recordPositionInPage + recordSize[1]);
      } else
        // Not a placeholder content record (nor a chunk head): the pointer does not lead where it used to.
        return NO_OFF_PAGE_FINGERPRINT;

      if (contentSize < 0 || contentOffset + contentSize > contentPage.getMaxContentSize())
        return NO_OFF_PAGE_FINGERPRINT;

      fingerprint = fnv1a(fingerprint, contentPage, contentOffset, contentSize);

      return fingerprint == NO_OFF_PAGE_FINGERPRINT ? EMPTY_OFF_PAGE_FINGERPRINT : fingerprint;
    } catch (final Exception e) {
      return unwalkableChunkChain(e, contentRID);
    }
  }

  /**
   * The one outcome every fingerprint walk shares: the record's off-page part could not be read, so there is nothing
   * to compare against - the head chunk stays unmergeable, and a placeholder's update stays unchecked at capture time
   * or is refused at commit time. Failing closed is always safe here, which is exactly why the catch that leads to it
   * must not become a place a real defect can hide: the assertion draws the line between "this database's chain is
   * broken" - which this method exists to tolerate, as {@link #findBrokenChunkChain} does - and a bug in the walk
   * itself, which under {@code -ea} (surefire's default) fails a test loudly instead of degrading into a page that is
   * silently never merged. It costs nothing in a production JVM, where the tolerance is unconditional.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private long unwalkableChunkChain(final Exception e, final Object walked) {
    // Deliberately named, rather than a positive list of what corrupted bytes may raise: the walk reads sizes and
    // offsets straight out of the page, so an I/O or bounds failure is data talking and must stay tolerated, while
    // neither of these two can come from the page content once the walk has checked for a page that is gone.
    assert !(e instanceof NullPointerException || e instanceof ClassCastException) :
        "the off-page fingerprint must only ever fail on an unreadable record: " + e;

    LogManager.instance().log(this, Level.FINE, "Unable to fingerprint the off-page content of %s", e, walked);
    return NO_OFF_PAGE_FINGERPRINT;
  }

  /**
   * The chunk-chain half of {@link #offPageContentFingerprint(RID, BasePage, boolean)}, starting from an
   * already-resolved head chunk. Never throws: a chain it cannot walk is simply one it cannot vouch for.
   *
   * @param headChunkHeaderPos offset of the head chunk's size field, i.e. right after its marker.
   */
  private long chunkChainTailFingerprint(final BasePage headChunkPage, final int headChunkHeaderPos) {
    try {
      return walkChunkChainTail(headChunkPage, headChunkHeaderPos);
    } catch (final Exception e) {
      return unwalkableChunkChain(e, headChunkPage.getPageId());
    }
  }

  private long walkChunkChainTail(final BasePage headChunkPage, final int headChunkHeaderPos) throws IOException {
    long pointer = headChunkPage.readLong(headChunkHeaderPos + INT_SERIALIZED_SIZE);
    if (pointer == 0)
      // A single-chunk record: its whole content is in the head chunk, which the pre-image check already covers.
      return EMPTY_OFF_PAGE_FINGERPRINT;

    long fingerprint = FNV_OFFSET_BASIS;
    final int totalPages = getTotalPages();
    // Exact loop detection, as in findBrokenChunkChain and for the same reason: a chain can legitimately hold more
    // chunks than the bucket has pages (chunks share pages after reuse), so any count-based bound either bails on a
    // valid chain or lets a corrupted one walk for a very long time first. Revisiting a pointer is the only certain
    // loop signal, and a chain that never revisits one is finite by construction. Allocated only if the chain turns
    // out to be long: a record of a handful of chunks - which is what every realistic one is - is the whole reason
    // this walk allocates nothing at all.
    LongHashSet visitedChunks = null;

    for (int chunk = 0; pointer > 0; ++chunk) {
      if (chunk >= CHUNKS_BEFORE_LOOP_DETECTION) {
        if (visitedChunks == null)
          visitedChunks = new LongHashSet();
        if (!visitedChunks.add(pointer))
          return NO_OFF_PAGE_FINGERPRINT;
      }

      final int chunkPageId = (int) (pointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (pointer % maxRecordsInPage);
      if (chunkPageId >= totalPages)
        return NO_OFF_PAGE_FINGERPRINT;

      final BasePage chunkPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), chunkPageId), pageSize);
      if (chunkPage == null)
        // The page the chain points at is gone: nothing to vouch for.
        return NO_OFF_PAGE_FINGERPRINT;
      final int chunkPositionInPageOffset = getRecordPositionInPage(chunkPage, chunkPositionInPage);
      if (chunkPositionInPageOffset == 0)
        return NO_OFF_PAGE_FINGERPRINT;

      final long[] chunkMarker = chunkPage.readNumberAndSize(chunkPositionInPageOffset);
      if (chunkMarker[0] != NEXT_CHUNK)
        return NO_OFF_PAGE_FINGERPRINT;

      final int headerPos = (int) (chunkPositionInPageOffset + chunkMarker[1]);
      final int chunkSize = chunkPage.readInt(headerPos);
      if (chunkSize < 0 || headerPos + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + chunkSize > chunkPage.getMaxContentSize())
        return NO_OFF_PAGE_FINGERPRINT;

      // The chunk's own identity goes in too: a chain relocated onto other slots is a different tail even when every
      // byte of content is the same.
      fingerprint = fnv1a(fingerprint, pointer);
      fingerprint = fnv1a(fingerprint, chunkSize);
      fingerprint = fnv1a(fingerprint, chunkPage, headerPos + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE, chunkSize);

      pointer = chunkPage.readLong(headerPos + INT_SERIALIZED_SIZE);
    }

    // A hash that lands on the "unknown" sentinel is moved onto the "no tail" one rather than being handed back as
    // unknown. The two therefore alias - a real tail can be indistinguishable from a single-chunk record - which is
    // sound because BOTH sides of the comparison go through this same remap, and is of the same ~2^-64 order as the
    // collision the fingerprint already accepts.
    return fingerprint == NO_OFF_PAGE_FINGERPRINT ? EMPTY_OFF_PAGE_FINGERPRINT : fingerprint;
  }

  /**
   * Folds {@code length} bytes of {@code page}, from {@code offset}, into {@code fingerprint}: the byte loop both
   * fingerprint walks share.
   * <p>
   * It reads the page one ABSOLUTE byte at a time rather than copying the range into a scratch array first, which is
   * what makes a fingerprint of any size allocate nothing - the walks run twice per update of the record they cover,
   * once when it is taken for update and once at commit, and a chunk or a content record is up to a page long. The
   * FNV loop has to touch every byte either way, so what the copy bought was a bulk read; what it cost was a
   * content-sized allocation per call. Absolute reads also leave the page buffer's position where they found it,
   * unlike the bulk read they replace.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static long fnv1a(final long fingerprint, final BasePage page, final int offset, final int length) {
    long folded = fingerprint;
    for (int i = 0; i < length; ++i)
      folded = (folded ^ (page.readByte(offset + i) & 0xFFL)) * FNV_PRIME;
    return folded;
  }

  /** Folds the 8 bytes of {@code value} into {@code fingerprint}, least significant first. */
  private static long fnv1a(final long fingerprint, final long value) {
    long folded = fingerprint;
    for (int shift = 0; shift < 64; shift += 8)
      folded = (folded ^ ((value >>> shift) & 0xFFL)) * FNV_PRIME;
    return folded;
  }

  /**
   * @return the page pinned for the record, or {@code null} when the RID could not name one - so a caller that needs
   * to read the record right after does not have to resolve the very same page a second time (#6129).
   */
  public MutablePage fetchPageInTransaction(final RID rid) throws IOException {
    if (rid.getPosition() < 0L) {
      LogManager.instance().log(this, Level.WARNING, "Cannot load a page from a record with invalid RID (" + rid + ")");
      return null;
    }

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount) {
        LogManager.instance().log(this, Level.WARNING, "Record " + rid + " not found");
      }
    }

    return database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
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
    if (cached > -1)
      // O(1) fast path: the counter is kept up to date on every commit, so return it directly. With an active
      // transaction add its uncommitted delta; without one there is nothing pending, so the cached value is the
      // committed count. Only a still-unknown (-1) counter falls through to the full recompute below.
      return cached + (transaction != null ? transaction.getBucketRecordDelta(fileId) : 0);

    // #5152: the recompute below (scan + publish of cachedRecordCount) must be mutually exclusive with a
    // commit's publishPages + record-count fold on this bucket. A commit holds this bucket's file lock across
    // both (TransactionContext.commit2ndPhase, until reset()), so acquiring the same lock here serializes the
    // recompute with commits. Without it a commit could publish a record and then, seeing the still-(-1)
    // counter, drop its delta at the fold while our scan misses that just-published record (lost update), or
    // conversely our scan counts a published record whose delta the fold then re-adds (double count). The lock
    // is taken only on this rare recompute path (counter unknown) - never on the O(1) cached fast-path returned
    // above. The requester mirrors the transaction's so a recompute invoked while the same transaction already
    // holds the lock re-enters instead of self-deadlocking.
    final TransactionManager txManager = database.getTransactionManager();
    // Deliberately the pure getRequester() read (not captureRequester()): this call acquires and releases the
    // lock symmetrically on one thread, so there is nothing to capture, and reading avoids mutating the
    // transaction's requester field from a possibly-foreign counting thread. Falls back to the current thread.
    final Object requester = transaction != null ? transaction.getRequester() : Thread.currentThread();
    final long lockTimeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    LockManager.LOCK_STATUS lockStatus = LockManager.LOCK_STATUS.NO;
    try {
      lockStatus = txManager.tryLockFile(fileId, lockTimeout, requester);

      // Another thread may have recomputed the counter while we were queued on the lock. Re-check now that we
      // hold it (or timed out) and skip the duplicate O(N) scan, which also shortens how long we hold the lock.
      final long recomputed = cachedRecordCount.get();
      if (recomputed > -1)
        return recomputed + (transaction != null ? transaction.getBucketRecordDelta(fileId) : 0);

      long total = 0;

      final int txPageCount = getTotalPages();

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

            // #6196: the same three shapes a scan hands out, and for the same reason - FIRST_CHUNK and not
            // isChunkHead(), because the head chunk of a placeholder's CONTENT is counted through its pointer.
            if (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK)
              total++;
          }
        }
      }

      // Publish the recomputed value only when this scan ran under the lock (acquired now, or already held by
      // an enclosing transaction). On a lock-acquisition timeout (NO) the scan ran lock-free and may be drifted,
      // so leave the counter at -1 and return a best-effort value: a later call recomputes cleanly.
      if (lockStatus != LockManager.LOCK_STATUS.NO)
        // The scan reads the transaction's view (getPage returns its uncommitted pages first), so `total`
        // already includes this transaction's pending delta. Cache the COMMITTED base (total - pending) so the
        // commit-time fold adds the delta exactly once instead of double-counting it; the caller still gets the
        // transaction-visible `total` below.
        cachedRecordCount.set(transaction != null ? total - transaction.getBucketRecordDelta(fileId) : total);
      else
        LogManager.instance().log(this, Level.FINE,
                "count() recompute on bucket '%s' ran lock-free after a %dms lock-acquisition timeout; result not cached", componentName,
                lockTimeout);
      return total;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot count bucket '" + componentName + "'", e);
    } finally {
      // Release only if WE acquired it. ALREADY_ACQUIRED means an enclosing transaction owns the lock and is
      // responsible for releasing it; NO means the acquisition timed out and we ran the scan lock-free.
      if (lockStatus == LockManager.LOCK_STATUS.YES)
        txManager.unlockFile(fileId, requester);
    }
  }

  /**
   * Walks every slot of the bucket, reports what it finds and - under {@code fix} - repairs it.
   * <p>
   * <b>Under {@code fix} this owns the transaction its repairs are made in</b> (#6320), begins it here and commits it
   * before returning, nested inside whatever the caller has open exactly as {@code GraphDatabaseChecker}'s passes and
   * {@code DatabaseChecker.checkDocuments} already do. It has to: the repairs are batched
   * ({@link RepairTransaction#commitBatchIfFull}) and a batch commit must never land on a transaction someone else is
   * filling. A read-only run touches nothing and keeps the caller's transaction, so a caller checking its own
   * uncommitted work still sees it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  public Map<String, Object> check(final int verboseLevel, final boolean fix) {
    // #6320: how many pages of repairs one transaction of this pass may accumulate before committing and opening the
    // next. Read once - it is a database-scoped setting and cannot change under a running check - and read here rather
    // than kept in a field because a bucket outlives any number of checks.
    final RepairTransaction repairTx = new RepairTransaction(database,
            fix ? RepairTransaction.configuredBatchPages(database) : 0);

    if (!fix)
      return checkInternal(verboseLevel, false, repairTx);

    // #6320: the counter count(*) answers from is invalidated BEFORE the first repair, not only after the last one.
    // A repair used to be one transaction, so nothing it did was durable until the end and the invalidation at the
    // end of checkInternal became visible with it; batching makes every batch durable as it commits, and the records
    // those batches removed are removed through deleteRecordInternal, which registers no bucket delta for the
    // commit-time fold to apply. Left until the end, the counter would go on serving the pre-repair number - to any
    // concurrent count(*), and to the next one after a run that failed part-way - for as long as the repair takes,
    // which on the backlogs this exists to clear is the whole point of the run (PR review). Invalidated up front, a
    // reader falls through to the authoritative scan instead. Still invalidated at the end as well: a count() that
    // recomputes mid-run caches what it saw, and the batches after it move on again.
    cachedRecordCount.set(-1);

    repairTx.begin();
    boolean completed = false;
    try {
      final Map<String, Object> stats = checkInternal(verboseLevel, true, repairTx);
      completed = true;
      return stats;
    } finally {
      repairTx.finish(completed);
    }
  }

  private Map<String, Object> checkInternal(final int verboseLevel, final boolean fix, final RepairTransaction repairTx) {
    final Map<String, Object> stats = new HashMap<>();

    final int totalPages = getTotalPages();

    if (verboseLevel > 1)
      LogManager.instance()
              .log(this, Level.INFO, "- Checking bucket '%s' (totalPages=%d spaceOnDisk=%s pageSize=%s)...", componentName, totalPages,
                      FileUtils.getSizeAsString((long) totalPages * pageSize), FileUtils.getSizeAsString(pageSize));

    final CheckStats totals = new CheckStats();

    // #6292: every placeholder POINTER the walk found, followed AFTER it rather than while it runs. Both questions a
    // pointer raises - does it still lead to a content record at all, and is that record the pre-#6196 ambiguous shape
    // - are about a slot the walk may not have reached yet, or may be about to repair away, so asking inline makes the
    // answer depend on nothing but the order the allocator happened to place the two slots in.
    final LongHashSet placeholderPointers = new LongHashSet();

    // #6294: the two halves of the mark-and-sweep that finds continuation chunks nothing points at. A chunk is
    // REACHABLE when a head whose slot survives this pass walks through it, and the walk that establishes that is the
    // one findBrokenChunkChain already makes for every head. One entry per chunk, on a pass that already reads every
    // page and walks every chain.
    final LongHashSet chunkSlots = new LongHashSet();
    final LongHashSet reachableChunks = new LongHashSet();
    // FAIL CLOSED, exactly as the edge-segment reclaim does: a chain walk that could not read a page, or a page or
    // slot the pass could not read at all, leaves live chunks unmarked - and an unmarked live chunk deleted as an
    // orphan is destroyed data. Any such gap disables the sweep entirely rather than shrinking it.
    boolean chunkReachabilityComplete = true;
    final ChunkChainWalk chainWalk = new ChunkChainWalk();

    // Positions this run repaired away, so the pointer pass can tell a pointer left dangling by the repair it has
    // already booked from one that was dangling before the run started (#6292).
    final LongHashSet repairedAwaySlots = new LongHashSet();

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
          // #4931: int*int overflowed here for buckets beyond 2^31 positions, and check(fix=true) then
          // deleted an innocent record at the wrong RID. recordPosition() widens to long.
          final RID rid = new RID(file.getFileId(), recordPosition(pageId, maxRecordsInPage, positionInPage));

          // #6293: what the slot IS, decided here and counted at the END of the block. It used to be counted inline,
          // by the branch that recognised it, with the repairs running afterwards - so a record CHECK DATABASE FIX
          // deleted during the run was reported both as a deleted record and as whatever category it had been before,
          // and one report described the same record twice. A category is a value now, and a slot the fix removed
          // simply carries the DELETED one.
          SlotCategory category = SlotCategory.UNCLASSIFIED;
          boolean allocated = false;

          final int recordPositionInPage = (int) page.readUnsignedInt(
                  PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);

          if (recordPositionInPage == 0) {
            // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
            category = SlotCategory.DELETED;

          } else if (recordPositionInPage > page.getContentSize()) {
            ++totals.totalErrors;
            // The slot's offset is not readable, so what it held was never known: it may have been a chunk head whose
            // chain nothing walked, and chunks nobody marked must not be mistaken for orphans.
            chunkReachabilityComplete = false;
            warning = "invalid record offset %d in page for record %s".formatted(recordPositionInPage, rid);
            if (fix) {
              deleteRecordInternal(rid, true, true, true);
              totals.deletedRecordsAfterFix.add(rid);
              repairedAwaySlots.add(rid.getPosition());
              category = SlotCategory.DELETED;
            }
          } else {

            try {
              final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

              allocated = true;
              // Reset HERE and not only inside the walk: a slot that is not a chunk head never walks anything, and a
              // stale set left from the previous head would then be attributed to it.
              chainWalk.reset();

              if (recordSize[0] == 0) {
                category = SlotCategory.DELETED;
              } else if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
                category = SlotCategory.PLACEHOLDER_POINTER;
                // Followed after the pass, not here: see placeholderPointers.
                placeholderPointers.add(rid.getPosition());
                recordSize[0] = MINIMUM_RECORD_SIZE;
              } else if (isChunkHead(recordSize[0])) {
                // #6196: a head chunk of either kind. Everything below is the same for the two - the chain, the walk
                // that checks it, the size the header declares - and the ONE thing that is not is what the slot is:
                // a record of its own, or the CONTENT of a placeholder, which is a surrogate exactly as a content
                // record small enough to fit its page is, and is counted as one rather than as an active record.
                final long headMarker = recordSize[0];
                category = headMarker == FIRST_CHUNK_PLACEHOLDER_CONTENT ?
                        SlotCategory.SURROGATE :
                        SlotCategory.MULTI_PAGE;

                // Walk the continuation chain to detect a structurally broken multi-page record (a dangling or
                // overwritten chunk pointer). check() otherwise validates only the first chunk's on-page size and
                // would never notice a broken chain, leaving the record undeletable by every normal path because
                // deleteRecordInternal throws the #4932 retry signal on it. On fix, force-delete it: the head slot is
                // freed so the record finally disappears - and since #6294 the chunks it can no longer reach are
                // freed too, by the sweep at the end of this method, rather than left to a compaction that never
                // collected them.
                final String chainProblem = findBrokenChunkChain(rid, page, recordPositionInPage, false, chainWalk);
                if (chainProblem != null) {
                  ++totals.totalErrors;
                  warning = "broken multi-page chunk chain for %srecord %s: %s".formatted(
                          headMarker == FIRST_CHUNK_PLACEHOLDER_CONTENT ? "placeholder content " : "", rid, chainProblem);
                  if (fix) {
                    // deletePlaceholderContent=true, so a content record is force-deleted here like any other head.
                    // The POINTER that referenced it is reconciled by the placeholder pass below, which removes it as
                    // part of this same repair instead of leaving a slot that a scan skips and count(*) counts.
                    deleteRecordInternal(rid, true, true, true);
                    totals.deletedRecordsAfterFix.add(rid);
                    repairedAwaySlots.add(rid.getPosition());
                    category = SlotCategory.DELETED;
                    allocated = false;
                    recordSize[0] = 0;
                  }
                }

                if (recordSize[0] == headMarker)
                  recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] == NEXT_CHUNK) {
                category = SlotCategory.CHUNK;
                chunkSlots.add(rid.getPosition());
                recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
                category = SlotCategory.SURROGATE;
                recordSize[0] *= -1;
              } else {
                category = SlotCategory.ACTIVE;
              }

              final long endPosition = recordPositionInPage + recordSize[1] + recordSize[0];
              if (endPosition > file.getPageSize()) {
                ++totals.totalErrors;
                warning = "wrong record size %d found for record %s".formatted(recordSize[1] + recordSize[0], rid);
                if (fix) {
                  deleteRecordInternal(rid, true, true, true);
                  totals.deletedRecordsAfterFix.add(rid);
                  repairedAwaySlots.add(rid.getPosition());
                  category = SlotCategory.DELETED;
                  allocated = false;
                }
              }

              if (endPosition > pageMaxOffset)
                pageMaxOffset = (int) endPosition;

              // #6294: the chunks a head walks through are LIVE only while that head is. One this pass repaired away
              // marks NOTHING, so its whole chain - including the chunks before the break - is swept at the source,
              // which is what the issue's "free them at the source" asks for without walking past a broken pointer.
              if (chainWalk.incomplete)
                chunkReachabilityComplete = false;
              else if (category != SlotCategory.DELETED)
                chainWalk.chunks.forEach(reachableChunks::add);

            } catch (final Exception e) {
              ++totals.totalErrors;
              warning = "unknown error on loading record %s: %s".formatted(rid, e.getMessage());

              // A slot that could not even be read may have been a chunk head, and what it reached is now unknown:
              // the sweep must not conclude that the chunks nothing else claims are unreachable.
              chunkReachabilityComplete = false;

              if (fix && !(e instanceof RecordNotFoundException)) {
                deleteRecordInternal(rid, true, true, true);
                totals.deletedRecordsAfterFix.add(rid);
                repairedAwaySlots.add(rid.getPosition());
                category = SlotCategory.DELETED;
                allocated = false;
              }
            }
          }

          // #6293: the ONE place a slot is counted, after every branch above has had its say about what it is and
          // after any repair has had its say about whether it still exists.
          if (allocated)
            totals.totalAllocatedRecords++;

          switch (category) {
            case DELETED -> {
              pageDeletedRecords++;
              totals.totalDeletedRecords++;
            }
            case ACTIVE -> {
              pageActiveRecords++;
              totals.totalActiveRecords++;
            }
            case PLACEHOLDER_POINTER -> {
              pagePlaceholderRecords++;
              totals.totalPlaceholderRecords++;
            }
            case SURROGATE -> {
              pageSurrogateRecords++;
              totals.totalSurrogateRecords++;
            }
            case MULTI_PAGE -> {
              // Not counted among the page's ACTIVE records, as it is not counted among the total ones: the two
              // tallies used to disagree on this one category, the per-page log calling a multi-page record active and
              // the returned totals keeping it separate.
              pageMultiPageRecords++;
              totals.totalMultiPageRecords++;
            }
            case CHUNK -> {
              pageChunks++;
              totals.totalChunks++;
            }
            case UNCLASSIFIED -> {
              // A slot too corrupted to classify that this run was not asked to repair: counted by nothing, exactly as
              // it was before, because there is no category it can honestly be put in.
            }
          }

          if (warning != null) {
            totals.warnings.add(warning);
            if (verboseLevel > 0)
              LogManager.instance().log(this, Level.SEVERE, "- " + warning);
            warning = null;
          }
        }

        totals.totalMaxOffset += pageMaxOffset;

        if (verboseLevel > 2)
          LogManager.instance().log(this, Level.FINE,
                  "-- Page %d records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d multiPageRecords=%d chunks=%d) maxOffset=%d",
                  pageId, recordCountInPage, pageActiveRecords, pageDeletedRecords, pagePlaceholderRecords, pageSurrogateRecords,
                  pageMultiPageRecords, pageChunks, pageMaxOffset);

      } catch (final Exception e) {
        ++totals.totalErrors;
        // The heads on a page that could not be read never walked their chains: their chunks are unmarked and must
        // not be mistaken for orphans.
        chunkReachabilityComplete = false;
        warning = "unknown error on checking page %d: %s".formatted(pageId, e.getMessage());
      }

      if (warning != null) {
        totals.warnings.add(warning);
        if (verboseLevel > 0)
          LogManager.instance().log(this, Level.SEVERE, "- " + warning);
        warning = null;
      }

      // #6320: at the PAGE boundary and nowhere inside it. The slot loop above reads its page through a reference
      // taken before any repair on it, and a commit half-way through would leave it reading an image whose
      // transaction is gone; here the next iteration takes a fresh one anyway.
      repairTx.commitBatchIfFull();
    }

    // Both reconciliations run AFTER the walk and against the state it left, which is what makes their answers
    // independent of the order the allocator happened to place the slots in - the same reason #6196 was reconciled
    // here rather than inline. Only the RETURNED totals are corrected; the per-page tallies the verbose log prints
    // have already gone out, and they describe the page as the walk found it, which is what a physical-layout log is
    // for.
    reconcilePlaceholderPointers(totals, placeholderPointers, repairedAwaySlots, totalPages, verboseLevel, fix, repairTx);
    reclaimOrphanedChunks(totals, chunkSlots, reachableChunks, chunkReachabilityComplete, verboseLevel, fix, repairTx);

    if (fix)
      // #5149: reconcile the cached record counter that count(*) relies on. Invalidating forces the next
      // count() to rescan authoritatively, and reusing the existing -1 sentinel means the value is
      // repopulated by count()'s own scan logic (no risk of a rule mismatch with this method's tallies) and
      // survives a rollback of the enclosing transaction. We invalidate rather than write the freshly scanned
      // value because check(fix=true) may run inside a caller-managed transaction: at commit TransactionContext
      // folds that transaction's accumulated bucket delta into the counter, but only when it is > -1. Writing a
      // scanned value would let unrelated inserts/deletes in that same transaction be double-counted on top of
      // it; leaving -1 makes the fold skip. (check()'s own corrupt-record deletions go through
      // deleteRecordInternal, which does not register a bucket delta, so they are not the concern here.)
      // Caveat: no count() must run on this bucket between here and the checker's commit - not from this caller
      // (DatabaseChecker does not) nor from a concurrent transaction. count() would repopulate the counter
      // (> -1) and let the commit-time fold re-apply the delta, reintroducing drift. That window (publishPages
      // then the fold in TransactionContext) is inherent to the incremental counter; CHECK FIX is an admin
      // operation, so it is expected to run without concurrent counts on the same bucket.
      cachedRecordCount.set(-1);

    final float avgPageUsed = totalPages > 0 ? ((float) totals.totalMaxOffset) / totalPages * 100F / pageSize : 0;

    if (verboseLevel > 1)
      LogManager.instance()
              .log(this, Level.INFO, "-- Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%",
                      totals.totalAllocatedRecords, totals.totalActiveRecords, totals.totalDeletedRecords,
                      totals.totalPlaceholderRecords, totals.totalSurrogateRecords, avgPageUsed);

    stats.put("pageSize", (long) pageSize);
    stats.put("totalPages", (long) totalPages);
    stats.put("totalAllocatedRecords", totals.totalAllocatedRecords);
    stats.put("totalActiveRecords", totals.totalActiveRecords);
    stats.put("totalPlaceholderRecords", totals.totalPlaceholderRecords);
    stats.put("totalSurrogateRecords", totals.totalSurrogateRecords);
    stats.put("totalDeletedRecords", totals.totalDeletedRecords);
    stats.put("totalMaxOffset", totals.totalMaxOffset);
    stats.put("totalMultiPageRecords", totals.totalMultiPageRecords);
    stats.put("totalChunks", totals.totalChunks);
    stats.put("orphanedChunks", totals.orphanedChunks);
    stats.put("orphanedChunksReclaimed", totals.orphanedChunksReclaimed);
    stats.put("danglingPlaceholderPointers", totals.danglingPlaceholderPointers);
    stats.put("danglingPlaceholderPointersFixed", totals.danglingPlaceholderPointersFixed);

    final DocumentType type = database.getSchema().getTypeByBucketId(fileId);
    if (type instanceof LocalVertexType) {
      stats.put("totalAllocatedVertices", totals.totalAllocatedRecords);
      stats.put("totalActiveVertices", totals.totalActiveRecords);
    } else if (type instanceof LocalEdgeType) {
      stats.put("totalAllocatedEdges", totals.totalAllocatedRecords);
      stats.put("totalActiveEdges", totals.totalActiveRecords);
    } else {
      stats.put("totalAllocatedDocuments", totals.totalAllocatedRecords);
      stats.put("totalActiveDocuments", totals.totalActiveRecords);
    }

    stats.put("deletedRecordsAfterFix", totals.deletedRecordsAfterFix);
    stats.put("warnings", totals.warnings);
    stats.put("autoFix", 0L);
    stats.put("totalErrors", totals.totalErrors);

    return stats;
  }

  /**
   * Follows every placeholder POINTER the walk of {@link #check} found, and answers the two questions a pointer raises
   * that its own slot cannot: does it still lead to a CONTENT record, and is that record the pre-#6196 ambiguous shape
   * a scan hands out twice.
   * <p>
   * <b>#6292 - the dangling pointer.</b> {@code check()} used to count a pointer as a placeholder and move on, never
   * following it. So a pointer whose content was gone - force-deleted by the very FIX that found its chain broken, or
   * lost to corruption or an interrupted repair - stayed on its page for good: {@code scan()} resolved it to nothing
   * and skipped it, {@code count()} counted the slot because a pointer IS a record as far as it is concerned, and this
   * method reported a clean database. Two counts of the same type disagreeing permanently, with nothing to say why.
   * Following the pointer costs the page fetch #6196 was already paying to read the target's marker; only the branch
   * that asks was missing.
   * <p>
   * A pointer left dangling by a deletion THIS run made is not booked as a second error: the record had one defect,
   * the FIX removed it, and the pointer is the other half of that removal rather than a new problem the operator has
   * to reconcile against the run that caused it.
   * <p>
   * <b>The repair frees the pointer SLOT and nothing else</b> ({@link #freeSlotOnly}), where the ordinary delete would
   * follow the pointer first: a pointer that is dangling because it was CORRUPTED names whatever record now lives at
   * that position, and deleting that record is exactly the damage this pass exists to prevent.
   *
   * @param repairedAwaySlots positions the walk removed, which is what tells a pointer orphaned by this run's own
   *                          repair from one that was already dangling when it started.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void reconcilePlaceholderPointers(final CheckStats totals, final LongHashSet placeholderPointers,
                                            final LongHashSet repairedAwaySlots, final int totalPages,
                                            final int verboseLevel, final boolean fix, final RepairTransaction repairTx) {
    if (placeholderPointers.isEmpty())
      return;

    // #6196: content records found behind a pointer while still wearing the ambiguous FIRST_CHUNK marker. The walk
    // counted each of them as a multi-page record of its own, which is precisely what they are NOT.
    final LongHashSet legacyContentHeads = new LongHashSet();
    /*
     * Of those, the ones MORE THAN ONE pointer leads to. The repair rests on the engine's own invariant - a placeholder
     * pointer references the content record written for it and nothing else - and this is the one way that invariant
     * can be caught failing without knowing what the bytes were meant to say: a content record belongs to exactly one
     * pointer, so a second one leading to the same slot is proof that a pointer is corrupted, and no reading of it
     * tells which. Refused rather than repaired, because the shape the repair would write is unrecoverable
     * information: it says whose the record is (code review on #6287).
     */
    final LongHashSet ambiguousHeadsWithSeveralPointers = new LongHashSet();

    for (final long pointerPosition : placeholderPointers.toArray()) {
      final RID pointerRID = new RID(fileId, pointerPosition);
      final long contentPosition = readPlaceholderPointer(totalPages, pointerPosition);
      if (contentPosition < 0)
        // The slot is no longer a pointer: this run's own repairs may have taken it, and a slot that is not a pointer
        // any more has nothing left for this pass to say about it.
        continue;

      switch (classifyPlaceholderTarget(totalPages, contentPosition)) {
      case CONTENT, UNREADABLE:
        // Nothing to do - and UNREADABLE deliberately with it: a target whose page could not be READ is an I/O fault,
        // not proof that the pointer leads nowhere, and this pass must never delete a pointer on that evidence.
        break;

      case LEGACY_AMBIGUOUS:
        if (!legacyContentHeads.add(contentPosition))
          ambiguousHeadsWithSeveralPointers.add(contentPosition);
        break;

      case DANGLING: {
        final RID contentRID = new RID(fileId, contentPosition);
        final boolean orphanedByThisRun = repairedAwaySlots.contains(contentPosition);

        ++totals.danglingPlaceholderPointers;
        if (!orphanedByThisRun)
          ++totals.totalErrors;

        // orphanedByThisRun implies fix: nothing is repaired away without it, so that branch always ends in a removal.
        String warning = orphanedByThisRun ?
                "placeholder pointer %s referenced the record %s this run removed: removed with it".formatted(pointerRID,
                        contentRID) :
                ("placeholder pointer %s leads to %s, which is not a content record: the record is invisible to every "
                        + "scan and still counted by count(*)%s").formatted(pointerRID, contentRID,
                        fix ? " - the pointer is removed" : "; run CHECK DATABASE FIX to remove the pointer");

        if (fix) {
          try {
            freeSlotOnly(pointerRID);
            totals.deletedRecordsAfterFix.add(pointerRID);
            ++totals.danglingPlaceholderPointersFixed;
            --totals.totalPlaceholderRecords;
            --totals.totalAllocatedRecords;
            ++totals.totalDeletedRecords;
          } catch (final Exception e) {
            if (orphanedByThisRun)
              // It was not going to be reported at all, and now it has to be: the pointer stays.
              ++totals.totalErrors;
            warning = "placeholder pointer %s leads to %s and could not be removed: %s".formatted(pointerRID, contentRID,
                    e.getMessage());
          }
        }

        totals.warnings.add(warning);
        if (verboseLevel > 0)
          LogManager.instance().log(this, Level.SEVERE, "- " + warning);
        // #6320: between two pointers, which is between two repairs - a freed pointer slot is one page and complete.
        repairTx.commitBatchIfFull();
        break;
      }
      }
    }

    for (final long contentPosition : legacyContentHeads.toArray()) {
      final RID contentRID = new RID(fileId, contentPosition);
      String warning;

      if (ambiguousHeadsWithSeveralPointers.contains(contentPosition)) {
        // Reported, never repaired: the counters are left saying multi-page record, which is what the slot itself
        // still says, because with two pointers leading here nothing in the bucket knows which of them is the lie.
        ++totals.totalErrors;
        warning = ("placeholder content record %s is stored as an ambiguous chunk chain (#6196) and is referenced by "
                + "more than one placeholder pointer: one of those pointers is corrupted, so the marker is left as it "
                + "is - repair the pointers first").formatted(contentRID);
      } else {
        --totals.totalMultiPageRecords;
        ++totals.totalSurrogateRecords;
        ++totals.totalErrors;

        // A repair, never a deletion: the record is intact, only the marker that says whose it is was never written.
        // Reported as an error whether or not it is fixed, because unfixed it is one a user can see - the content is
        // returned twice by a scan, under two different RIDs.
        warning = "placeholder content record %s is stored as an ambiguous chunk chain (#6196) and is returned twice by a scan%s"
                .formatted(contentRID, fix ? ": repaired" : "; run CHECK DATABASE FIX to repair it");
        if (fix && !repairLegacyContentHead(contentRID)) {
          --totals.totalSurrogateRecords;
          ++totals.totalMultiPageRecords;
          warning = "placeholder content record %s is stored as an ambiguous chunk chain (#6196) and could not be repaired"
                  .formatted(contentRID);
        }
      }

      totals.warnings.add(warning);
      if (verboseLevel > 0)
        LogManager.instance().log(this, Level.SEVERE, "- " + warning);
      // Between two content heads, and a no-op on a run without FIX: the batch pages are 0 unless there are repairs.
      repairTx.commitBatchIfFull();
    }
  }

  /**
   * Frees every continuation chunk no chain reaches (#6294): a mark-and-sweep over the marks the single pass of
   * {@link #check} already collects, which is the same shape {@code GraphDatabaseChecker} uses for orphaned edge
   * segments, down to failing closed.
   * <p>
   * Force-deleting a record with a broken chunk chain frees its HEAD slot only, and three comments in this file used
   * to promise the rest would be "reclaimed by compaction or a database check". Neither did: this method counted a
   * {@code NEXT_CHUNK} slot and moved on, and {@code compressPage} re-flows a page's live slots - an orphaned chunk
   * still HAS a live slot entry, so it was re-flowed along with everything else rather than dropped. The leak was
   * bounded per incident and permanent, surviving every repair short of an export and reimport.
   * <p>
   * Reachability cannot be answered from a chunk's own slot - the same asymmetry that made #6196's content records
   * need a marker of their own - so it is answered from the other end: the chain walk every head already makes marks
   * the chunks that head reaches, and a head this run repaired away marks nothing, which frees its chunks at the
   * source as well.
   * <p>
   * <b>Fails closed.</b> An unmarked LIVE chunk deleted as an orphan is destroyed data, so any gap in the marking -
   * a page the walk could not read, a chain walk that stopped on an I/O fault, a slot that could not be classified -
   * disables the sweep entirely rather than shrinking it. That is the same rule the edge-segment reclaim follows and
   * for the same reason.
   * <p>
   * <b>A leak is not a corruption</b>, so an orphan is a COUNT and never an error or a per-chunk warning: no record is
   * wrong, no query is affected, no two counts disagree - the bucket is simply carrying dead space.
   * {@code orphanedEdgeSegments} and {@code orphanedExternalRecords} are reported exactly that way, and the scale is
   * the reason it matters here rather than being a matter of taste: measured on {@code CRUDTest.multiUpdatesOverlap},
   * a bucket of 1.5M chunks carries 243821 orphans, and one warning apiece would be a report nobody can read built
   * out of a quarter of a million strings.
   * <p>
   * <b>What it may spend</b> is bounded by {@link RepairTransaction#commitBatchIfFull}, shared with every other repair of the run
   * since #6320, where it used to keep a memory budget of its own and stop at it - leaving the rest of the backlog for
   * the next {@code FIX}. Committing the batch gives the pages back instead, so the whole backlog goes in one run.
   * <p>
   * <b>Same precondition the rest of {@code check(fix)} already carries</b>: it is an ADMIN operation, expected to run
   * without concurrent writers on the bucket. The marks are collected page by page, so a chain rewritten by a
   * concurrent commit half-way through the walk can leave its new chunk unmarked; the very same window is what lets
   * the broken-chain branch force-delete a record whose chain a concurrent writer was rebuilding, and what
   * {@code GraphDatabaseChecker}'s orphan reclaim and {@code count()}'s counter reconciliation are both stated to
   * require. This adds no exposure of its own, and buys none back either.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void reclaimOrphanedChunks(final CheckStats totals, final LongHashSet chunkSlots,
                                     final LongHashSet reachableChunks, final boolean reachabilityComplete,
                                     final int verboseLevel, final boolean fix, final RepairTransaction repairTx) {
    if (chunkSlots.isEmpty())
      return;

    if (!reachabilityComplete) {
      // Said out loud rather than left as a zero: "no orphans found" and "could not tell" are different answers, and
      // a report that gives the first when it means the second is the reason to look for orphans in the first place.
      final String warning = ("the orphaned-chunk sweep of bucket '%s' was skipped: the reachability walk did not "
              + "complete, so an unmarked chunk is not proof of an orphan - repair the errors reported above and run "
              + "CHECK DATABASE again").formatted(componentName);
      totals.warnings.add(warning);
      if (verboseLevel > 0)
        LogManager.instance().log(this, Level.WARNING, "- " + warning);
      return;
    }

    for (final long chunkPosition : chunkSlots.toArray()) {
      if (reachableChunks.contains(chunkPosition))
        continue;

      // Counted whether or not it is freed: "how much is leaked" and "how much this run gave back" are different
      // questions, and orphanedChunks/orphanedChunksReclaimed are the two answers.
      ++totals.orphanedChunks;

      if (!fix)
        continue;

      final RID chunkRID = new RID(fileId, chunkPosition);
      try {
        // The chain this chunk belonged to no longer exists to be walked, so only its own slot is freed.
        freeSlotOnly(chunkRID);
        // Deliberately NOT added to deletedRecordsAfterFix: that list is the RIDs of RECORDS this run removed, and a
        // continuation chunk is a fragment of one, under a RID no caller ever held.
        ++totals.orphanedChunksReclaimed;
        --totals.totalChunks;
        --totals.totalAllocatedRecords;
        ++totals.totalDeletedRecords;
      } catch (final Exception e) {
        // A warning, unlike the reclaim itself: this one is a repair that did not happen, which is worth telling the
        // operator about, where a chunk that WAS reclaimed is worth nothing more than the counter above.
        final String warning = "chunk %s is reachable from no record and could not be reclaimed: %s".formatted(chunkRID,
                e.getMessage());
        totals.warnings.add(warning);
        if (verboseLevel > 0)
          LogManager.instance().log(this, Level.SEVERE, "- " + warning);
      }

      // #6320: between two chunks, which is between two repairs. The memory this sweep can hold is what its own budget
      // used to bound by STOPPING; giving the pages back instead is what lets a backlog of any size be reclaimed by ONE
      // run rather than by as many runs as the backlog divided by the budget.
      repairTx.commitBatchIfFull();
    }
  }

  /**
   * The position a placeholder POINTER slot references, or {@code -1} when that slot no longer holds a pointer at all
   * (deleted, rewritten, or repaired away since the walk saw it). Never throws: an unreadable slot answers {@code -1}
   * like a missing one, and the caller leaves it alone either way.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private long readPlaceholderPointer(final int totalPages, final long pointerPosition) {
    try {
      final int pageId = (int) (pointerPosition / maxRecordsInPage);
      if (pageId >= totalPages)
        return -1L;

      final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
      final int positionInPage = (int) (pointerPosition % maxRecordsInPage);
      if (positionInPage >= page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return -1L;

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        return -1L;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] != RECORD_PLACEHOLDER_POINTER)
        return -1L;

      return page.readLong((int) (recordPositionInPage + recordSize[1]));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to re-read the placeholder pointer at position %d of bucket '%s'", e,
              pointerPosition, componentName);
      return -1L;
    }
  }

  /**
   * Frees ONE slot and nothing else: the slot-table entry is zeroed, the record dropped from the transaction's cache
   * and both commit-time page merges excluded, exactly as every non-plain shape of {@link #deleteRecordInternal}
   * excludes them.
   * <p>
   * The two repairs {@link #check} makes that must NOT cascade use it. A dangling placeholder POINTER (#6292) cannot
   * go through the ordinary delete, which follows the pointer first: a pointer is dangling either because its content
   * is gone - nothing to follow - or because it was CORRUPTED, in which case it now names some unrelated record that
   * the ordinary delete would remove. An orphaned continuation chunk (#6294) has no chain left to walk either, and
   * only its own slot to give back.
   * <p>
   * No page statistics are updated here, and since #6339 that is a decision rather than an omission: what the page
   * has to offer once this slot is gone is settled by the compression the commit runs on it, which reports it
   * ({@link #compressPageInternal}). Until then the slot is a hole the allocator could not use anyway.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void freeSlotOnly(final RID rid) throws IOException {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    final TransactionContext slotTx = database.getTransactionIfExists();
    if (slotTx != null) {
      slotTx.poisonEdgeAppendPage(fileId, pageId);
      if (slotTx.isSlotMergeEnabled())
        slotTx.poisonSlotRebasePage(fileId, pageId);
    }

    database.getTransaction().removeRecordFromCache(rid);

    final MutablePage page = database.getTransaction()
            .getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
    page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
    freedWithoutClaiming(page);

    database.getTransaction().addDeletedRecord(rid);
  }

  /**
   * What the slot at {@code contentPosition} - the target of a placeholder POINTER - turns out to hold. Only
   * {@link #check} asks, and only about a slot a pointer led it to, so it costs one page fetch per placeholder
   * POINTER - every one of them, because the marker is the only thing that tells the shapes apart and reading it is
   * the whole question. That is the deliberate trade, and the scale to judge it against is what {@code check} already
   * spends on the same pass: it reads every page of the bucket and walks the FULL continuation chain of every
   * multi-page record. A pre-#6149 bucket dense with placeholders pays one more fetch each, of pages the walk itself
   * touches anyway, on an operation that is already O(pages + chunks).
   * <p>
   * It is paid on every run, including on a database that has already been repaired and on one this build created -
   * and the obvious saving, a persisted "no legacy shape here" flag set by a successful FIX, is deliberately not taken.
   * Such a flag would have to stay true through every way old bytes can arrive in a repaired database - a restore from
   * an older backup, an HA follower resynced from an older leader's snapshot, a file opened by an older binary and
   * handed back - and a flag that is wrong makes {@code check} skip the one question that finds the shape, which is
   * the failure this whole marker exists to end. A bounded, honest cost on an admin operation beats a cheap answer
   * that can be stale (code review on #6287).
   * <p>
   * Never throws: a target whose page cannot be read answers {@link PlaceholderTarget#UNREADABLE}, which is not
   * evidence of anything and is acted on by nobody.
   *
   * @param totalPages the page count {@link #check} snapshotted before its walk, so every question it asks is answered
   *                   against the same bucket the walk itself covered, rather than against a count that may have moved
   *                   under it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private PlaceholderTarget classifyPlaceholderTarget(final int totalPages, final long contentPosition) {
    try {
      if (contentPosition < 0)
        return PlaceholderTarget.DANGLING;
      final int contentPageId = (int) (contentPosition / maxRecordsInPage);
      if (contentPageId >= totalPages)
        return PlaceholderTarget.DANGLING;

      final BasePage contentPage = database.getTransaction()
              .getPage(new PageId(database, file.getFileId(), contentPageId), pageSize);
      final int positionInPage = (int) (contentPosition % maxRecordsInPage);
      if (positionInPage >= contentPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return PlaceholderTarget.DANGLING;

      final int recordPositionInPage = getRecordPositionInPage(contentPage, positionInPage);
      if (recordPositionInPage == 0)
        return PlaceholderTarget.DANGLING;

      final long marker = contentPage.readNumberAndSize(recordPositionInPage)[0];
      if (marker == FIRST_CHUNK)
        return PlaceholderTarget.LEGACY_AMBIGUOUS;
      // <= and not the < every other classification site uses, and deliberately so: this is the ONE site whose answer
      // costs a record. RECORD_PLACEHOLDER_CONTENT (-5) is the boundary the class doc draws between the sizes and the
      // markers, and it is the one value on it - a content record of exactly MINIMUM_RECORD_SIZE bytes. Nothing else
      // in the engine writes -5 (the markers stop at -4 and a plain size is positive), so a slot holding it can only
      // be somebody's content, and calling it DANGLING here would have FIX delete a live pointer. Measured: the
      // smallest document this serializer produces is 6 bytes, so the value is not reachable today and
      // createRecordInternal/updateRecordInternal now pad content past it - this is the third guard, on the only path
      // where being wrong is unrecoverable (PR review).
      if (marker == FIRST_CHUNK_PLACEHOLDER_CONTENT || marker <= RECORD_PLACEHOLDER_CONTENT)
        return PlaceholderTarget.CONTENT;
      return PlaceholderTarget.DANGLING;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to inspect the content record at position %d of bucket '%s'", e,
              contentPosition, componentName);
      return PlaceholderTarget.UNREADABLE;
    }
  }

  /**
   * Rewrites the marker of a legacy ambiguous content head (see {@link #classifyPlaceholderTarget}) into
   * {@link #FIRST_CHUNK_PLACEHOLDER_CONTENT}, which is the whole of the repair: the record's bytes, its chunk header,
   * its chain and its RID are all correct and untouched, and the one thing that was never written down is which of the
   * two kinds of head this is.
   * <p>
   * The rewrite is refused unless the new marker occupies EXACTLY the bytes the old one does. Both are one zigzag byte
   * as {@code writeNumber} produces them, so the refusal is unreachable through a marker this engine wrote; it exists
   * for the denormalised encoding a corrupted or hand-patched page can hold, where a shorter marker would leave the
   * chunk header a byte adrift and cost the record its chain.
   * <p>
   * What it takes on trust is the engine's own invariant: a placeholder POINTER references the CONTENT record written
   * for it and nothing else. A pointer corrupted into referencing an unrelated multi-page record would have this hide
   * that record from scans - but such a database is already handing that record's bytes out through the placeholder,
   * and unlike the force-delete {@code check(fix)} performs on a broken chain, this is reversible: not one byte of the
   * record, its chunk header or its chain is touched, so rewriting the marker back restores it exactly.
   * <p>
   * The one form of that corruption which can be RECOGNISED rather than merely tolerated - two pointers leading to the
   * same slot, which a healthy bucket cannot produce - is refused by the caller before it gets here, and reported
   * instead. See {@code ambiguousHeadsWithSeveralPointers} in {@link #check}.
   *
   * @return true when the marker was rewritten.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private boolean repairLegacyContentHead(final RID contentRID) {
    try {
      final int contentPageId = (int) (contentRID.getPosition() / maxRecordsInPage);
      final int positionInPage = (int) (contentRID.getPosition() % maxRecordsInPage);
      final MutablePage contentPage = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), contentPageId), pageSize, false);

      final int recordPositionInPage = getRecordPositionInPage(contentPage, positionInPage);
      final long[] recordSize = contentPage.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] != FIRST_CHUNK
              || recordSize[1] != Binary.getNumberSpace(FIRST_CHUNK_PLACEHOLDER_CONTENT))
        return false;

      // Not a single-slot change any merge could replay: the marker decides what the slot IS.
      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled())
        slotTx.poisonSlotRebasePage(fileId, contentPageId);

      contentPage.writeNumber(recordPositionInPage, FIRST_CHUNK_PLACEHOLDER_CONTENT);
      return true;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Unable to repair the placeholder content record %s of bucket '%s'", e,
              contentRID, componentName);
      return false;
    }
  }

  /**
   * Fires the BEFORE READ listeners of the database and of the RID's type, and refuses to fire them AGAIN while one
   * of them is running on this thread.
   * <p>
   * The re-entrancy is not hypothetical: this dispatch happens from inside the read, so a listener that loads
   * anything at all re-enters {@link #getRecordInternal} and fires itself. Without the guard that recursion ends in
   * a {@code StackOverflowError} - which is exactly what a {@code BEFORE READ} trigger used to do, and what the
   * body of any READ trigger can still ask for, since that body is arbitrary SQL or JavaScript and
   * {@code SELECT FROM SameType} is a reasonable thing to write in one.
   * <p>
   * The rule the guard implements is the ordinary one: a listener's own reads do not fire listeners. It costs
   * nothing when nobody is listening - the two {@code hasBeforeReadListeners()} probes are field reads, and the
   * thread-local is not touched at all in that case, which is every read on a database with no read listener.
   * <p>
   * The flag lives on {@code DatabaseContextTL} rather than in a thread-local of its own so that it is scoped per
   * DATABASE as well as per thread; see the field's javadoc for why that distinction matters.
   */
  private boolean fireBeforeReadEvents(final RID rid) {
    final RecordEventsRegistry databaseEvents = (RecordEventsRegistry) database.getEvents();
    final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
    final RecordEventsRegistry typeEvents = type != null ? (RecordEventsRegistry) type.getEvents() : null;

    if (!databaseEvents.hasBeforeReadListeners() && (typeEvents == null || !typeEvents.hasBeforeReadListeners()))
      return true;

    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    if (context == null)
      // No context on this thread for this database, so there is no re-entrancy to track. Reads reach a bucket
      // through the database, which establishes one, so this is the defensive branch rather than a live path.
      return dispatchBeforeRead(rid, databaseEvents, typeEvents);

    if (context.isFiringReadEvents())
      // A listener is reading. Let the read through untouched - vetoing it here would break the listener's own
      // query, and firing again is the recursion this exists to stop.
      return true;

    context.setFiringReadEvents(true);
    try {
      return dispatchBeforeRead(rid, databaseEvents, typeEvents);
    } finally {
      context.setFiringReadEvents(false);
    }
  }

  private boolean dispatchBeforeRead(final RID rid, final RecordEventsRegistry databaseEvents,
      final RecordEventsRegistry typeEvents) {
    if (!databaseEvents.onBeforeRead(rid))
      return false;
    return typeEvents == null || typeEvents.onBeforeRead(rid);
  }

  /**
   * The caller should call @{@link DatabaseInternal#invokeAfterReadEvents(Record)} after created the record and manage the result correctly.
   */
  public Binary getRecordInternal(final RID rid, final boolean readPlaceHolderContent) {
    // INVOKE EVENT CALLBACKS
    if (!fireBeforeReadEvents(rid))
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

      // #6196: a placeholder CONTENT record is refused to a caller that did not ask for one, in BOTH the shapes it can
      // be stored in - a negated size when a page could host it whole, and a chunk head when none could. The two are
      // separated below only because the negated one has to be turned back into a size, while the chunk head is read
      // by the chain walk exactly as a record's own head chunk is.
      if (isPlaceholderContent(recordSize[0]) && !readPlaceHolderContent)
        // PLACEHOLDER CONTENT: NOT A RECORD OF ITS OWN
        return null;

      if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT)
        recordSize[0] *= -1;

      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
        // FOUND PLACEHOLDER, LOAD THE REAL RECORD
        final RID placeHolderPointer = new RID(rid.getBucketId(),
                page.readLong((int) (recordPositionInPage + recordSize[1])));
        return getRecordInternal(placeHolderPointer, true);
      } else if (isChunkHead(recordSize[0])) {
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

    // A CONTENT record stores its size NEGATED, so its body may not be short enough to write a marker: not < 5 (which
    // would write a -1..-4 the marker namespace owns) and not exactly 5 either (which would write the -5 BOUNDARY that
    // every reader excludes with a strict `<`). A record of its own has no such constraint - its size is positive -
    // but it is padded to the same floor, as it always has been. Fill the difference with blanks, which the
    // deserializer stops before: it reads the property count, not the slot length.
    while (buffer.size() < MINIMUM_RECORD_SIZE || (isPlaceHolder && buffer.size() == MINIMUM_RECORD_SIZE)) {
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
        final PageAnalysis pageAnalysis = findAvailableSpace(-1, spaceNeeded, txPageCounter, false, -1);
        foundPage = pageAnalysis.page;
        newRecordPositionInPage = pageAnalysis.newRecordPositionInPage;
        availablePositionIndex = pageAnalysis.availablePositionIndex;
        createNewPage = pageAnalysis.createNewPage;

        if (!createNewPage) {
          final int reservedSlot = claimInsertSlot(foundPage, pageAnalysis, availablePositionIndex,
                  newRecordPositionInPage, spaceNeeded);
          if (reservedSlot < 0)
            // ONCE THE IN-FLIGHT RESERVATIONS ARE COUNTED THE PAGE CANNOT HOST THE RECORD ANYMORE: USE A NEW PAGE
            createNewPage = true;
          else
            availablePositionIndex = reservedSlot;
        }
      } else
        createNewPage = true;

      final MutablePage selectedPage;
      if (createNewPage) {
        // Atomically reserve the next page number to prevent two concurrent transactions
        // from allocating the same page number (which would cause silent data corruption
        // when their commits both succeed and overwrite each other's chunks).
        final int reservedPageNumber = reservedPageCounter.getAndIncrement();
        selectedPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
        newRecordPositionInPage = contentHeaderSize;
        availablePositionIndex = 0;
      } else
        selectedPage = database.getTransaction().getPageToModify(foundPage);

      LogManager.instance()
              .log(this, Level.FINE, "Creating record (%s records=%d threadId=%d)", selectedPage, availablePositionIndex,
                      Thread.currentThread().getId());
      final RID rid = new RID(file.getFileId(),
              ((long) selectedPage.getPageId().getPageNumber()) * maxRecordsInPage + availablePositionIndex);

      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newRecordPositionInPage;
      final boolean singleSlotInsert = !createNewPage && !isPlaceHolder && spaceNeeded <= spaceAvailableInCurrentPage;

      // #5596: a plain insert into a free slot of a REUSED page is exactly what the disjoint-slot merge replays
      // (writeRecordAtSlot writes the same three things: the slot pointer, the record count and the record itself),
      // so declare those bytes covered by it. Everything else here - a multi-page record, a placeholder, a record on
      // a brand-new page - stays undeclared, which is what makes a forgotten poison call harmless.
      final int previousCoverage = selectedPage.beginCoveredWrite(singleSlotInsert ? MutablePage.COVERAGE_SLOT_MERGE : 0);
      final short recordCountInPage;
      try {
        // RESERVE A SPOT IMMEDIATELY TO AVOID USAGE FOR MULTI PAGE RECORD
        selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
                newRecordPositionInPage);

        short currentRecordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        if (availablePositionIndex + 1 > currentRecordCountInPage) {
          // UPDATE RECORD NUMBER
          currentRecordCountInPage = (short) (availablePositionIndex + 1);
          selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, currentRecordCountInPage);
        }
        recordCountInPage = currentRecordCountInPage;

        if (spaceNeeded > spaceAvailableInCurrentPage) {
          // MULTI-PAGE RECORD: a brand-new chunk chain also writes this page's record table and record count, which no
          // tracked slot image accounts for. Two things keep it out of the merge, and both are outside this call:
          // singleSlotInsert is false here, so the declaration opened above is 0 and every byte written stays
          // uncovered; and on a REUSED page the `if (!singleSlotInsert) poisonSlotRebasePage` at the end of this
          // method excludes it outright. A brand-new page needs neither - it is never rebase-tracked.
          //
          // The record is being created at the page's content end, so it owns nothing yet: the head chunk takes the
          // whole free tail and seals the page (#6154).
          //
          // #6196: this is the ONE place a placeholder's CONTENT record becomes a chunk chain (an update of one that
          // no longer fits refuses to spill and has the caller recreate it here), so it is the one place the head
          // chunk has to be told it is content rather than a record. Everything the negated size marker would have
          // said about the slot is said by the marker this stamps, and by nothing else.
          writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage, 0, 0,
                  isPlaceHolder);

        } else {
          final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage,
                  isPlaceHolder ? (-1L * bufferSize) : bufferSize);
          selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(),
                  bufferSize);
          updatePageStatistics(selectedPage, spaceAvailableInCurrentPage, -spaceNeeded);
        }
      } finally {
        selectedPage.endCoveredWrite(previousCoverage);
      }

      LogManager.instance()
              .log(this, Level.FINE, "Created record %s (%s records=%d threadId=%d)", rid, selectedPage, recordCountInPage,
                      Thread.currentThread().getId());

      // DISJOINT-SLOT MERGE (#5381): a brand-new record inserted into a FREE slot of an EXISTING (reused) page
      // commutes with concurrent writes to other slots of that page. Track it so a commit-time page-version
      // conflict can be resolved by replaying this insert on the newer committed page (see TransactionContext).
      // A record on a brand-new page (createNewPage) has no existing-page conflict to rebase; a multi-page or
      // placeholder record is not a plain single-slot insert, so it poisons the page instead.
      // #5279: an edge-list segment is tracked here too, unlike on the UPDATE path. Allocating a brand-new chunk
      // is an ordinary free-slot insert whose whole content this transaction wrote, so it is replayable; and it
      // is the dominant write of "create a vertex and its first edges", which otherwise made every concurrent
      // case-creation collide on the edge-segment bucket even though the chunks are unrelated.
      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled() && !createNewPage) {
        final int slotPageNumber = selectedPage.getPageId().getPageNumber();
        if (!singleSlotInsert)
          slotTx.poisonSlotRebasePage(fileId, slotPageNumber);
        // Skip the record-image copy on a page that is already poisoned (it would be discarded anyway).
        else if (!slotTx.isSlotRebasePagePoisoned(fileId, slotPageNumber))
          slotTx.trackRebasableInsert(fileId, slotPageNumber, availablePositionIndex,
                  Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(), buffer.getContentBeginOffset() + bufferSize));
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + componentName + "'", e);
    }
  }

  /**
   * Emergency repair primitive: force-writes {@code record} into the EXPLICIT slot identified by {@code position},
   * bypassing the normal space allocator entirely. Used to recreate a record at the exact RID it held before an
   * out-of-band deletion (a raw delete that skipped graph cascade, or physical corruption), so existing references
   * to that RID - edge endpoints, adjacency-list entries - become valid again without rewriting every one of them.
   * <p>
   * Refuses if the slot is occupied by a live record (this can never silently overwrite data), or if its page does
   * not already exist (a RID that never held a record on this bucket has nothing to restore). A record too large
   * to fit in a single page is written as a multi-page chunk chain, same as a normal insert would - only the
   * FIRST chunk is pinned to the exact target slot (see {@link #writeMultiPageRecord}); continuation chunks need
   * no fixed position and are placed by the normal allocator, possibly on other pages. This includes position 0 of
   * the bucket: {@code getAvailableSpaceInPage}'s "never CHOOSE position 0 for a multi-page record" preference
   * (a legacy sentinel-collision guard - {@code nextChunkPointer == 0} also means "no next chunk") only applies
   * when the allocator is free to pick a different slot for a CONTINUATION chunk; it is never consulted for the
   * head/first chunk, whose position is fixed by the caller here exactly as it already is by an UPDATE that grows
   * an existing record into a chunk chain in place (same {@link #writeMultiPageRecord} call, no special-casing).
   * <p>
   * Unlike a normal allocator-driven insert, the content-insertion offset is computed directly against the target
   * page's CURRENT record count rather than via the allocator's free-page search - that search exists to let an
   * insert move to a DIFFERENT page when this one lacks room or its slot table is at capacity, which is not an
   * option here: the caller already committed to an exact slot on this exact page.
   * <p>
   * Always poisons the page for the commit-time disjoint-slot merge (unlike a normal allocator-driven insert):
   * this write's target slot was not discovered via the normal free-space search the merge's replay assumes, so any
   * real commit-time conflict on this page falls back to a full-transaction retry instead of attempting a replay.
   *
   * @throws DatabaseOperationException if the slot is occupied or its page does not exist.
   */
  public RID restoreRecordAtPosition(final long position, final Record record) {
    final int positionInPage = (int) (position % maxRecordsInPage);

    try {
      // Carries the CREATE_RECORD permission check, so there is no separate one here (PR #6130 review). Serializing
      // the record only afterwards means an unauthorized caller, a missing page and an occupied slot are all
      // rejected before anything is built.
      final MutablePage selectedPage = checkRestoreTargetIsFree(position);

      final Binary buffer = database.getSerializer().serialize(database, record);
      while (buffer.size() < MINIMUM_RECORD_SIZE)
        buffer.append((byte) 0);
      final int bufferSize = buffer.size();
      final int spaceNeeded = Binary.getNumberSpace(bufferSize) + bufferSize;

      final short currentRecordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      final int newRecordPositionInPage = findContentInsertionOffset(selectedPage, currentRecordCountInPage);
      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newRecordPositionInPage;

      final RID rid = new RID(file.getFileId(), position);

      LogManager.instance()
              .log(this, Level.WARNING, "Restoring record %s at its original position (records=%d threadId=%d)", rid,
                      positionInPage, Thread.currentThread().getId());

      // Out-of-band write: always poison, never attempt a disjoint-slot-merge replay of this insert (see javadoc).
      final int previousCoverage = selectedPage.beginCoveredWrite(0);
      try {
        selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, newRecordPositionInPage);

        if (positionInPage + 1 > currentRecordCountInPage)
          selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (positionInPage + 1));

        if (spaceNeeded > spaceAvailableInCurrentPage) {
          // MULTI-PAGE RECORD: only the first chunk is pinned to this slot; writeMultiPageRecord places the rest.
          // Like a normal create, the restored record starts at the page's content end and owns nothing yet (#6154).
          // A record restored at an explicit RID is a record of its own, never a placeholder's content: the content of
          // a placeholder has no RID anyone outside the bucket knows to ask for.
          writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage, 0, 0,
                  false);
        } else {
          final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage, bufferSize);
          selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
          updatePageStatistics(selectedPage, spaceAvailableInCurrentPage, -spaceNeeded);
        }
      } finally {
        selectedPage.endCoveredWrite(previousCoverage);
      }

      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled())
        slotTx.poisonSlotRebasePage(fileId, selectedPage.getPageId().getPageNumber());

      ((RecordInternal) record).setBuffer(buffer.getNotReusable());
      ((RecordInternal) record).setIdentity(rid);

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot restore record at position " + position + " in bucket '" + componentName + "'", e);
    }
  }

  /**
   * The two preconditions {@link #restoreRecordAtPosition} enforces before it writes anything: {@code position}'s
   * page must exist, and its slot must not already hold a live record. So a restore can never silently overwrite
   * data.
   * <p>
   * Public because {@code LocalDatabase.restoreRecord} calls it FIRST, ahead of the record's own validation and the
   * beforeCreate listeners (#6127 review): aiming at a RID that is still live is the likeliest mistake to make with
   * this statement, and it used to be the only error a restore could return - reporting a mandatory-property
   * violation instead would send the caller off to fix the wrong thing. That early call is a diagnostic ordering
   * only: it does not license the write, because {@code restoreRecordAtPosition} repeats this check on the page it
   * is about to write, inside the same transaction, and that later call is the one the write depends on.
   *
   * @return the page holding the target slot, already fetched for modification.
   *
   * @throws DatabaseOperationException if the page does not exist or the slot is occupied by a live record.
   */
  public MutablePage checkRestoreTargetIsFree(final long position) {
    // Gated on the same permission the write is: this runs BEFORE the record's own validation, so without it a
    // caller with no CREATE_RECORD on the bucket could use the two error messages to probe which slots are live.
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    final int pageId = (int) (position / maxRecordsInPage);
    final int positionInPage = (int) (position % maxRecordsInPage);

    try {
      if (pageId >= getTotalPages())
        throw new DatabaseOperationException(
            "Cannot restore record at position " + position + " in bucket '" + componentName + "': its page (" + pageId
                + ") does not exist");

      final MutablePage selectedPage = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);

      final short currentRecordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage < currentRecordCountInPage) {
        final int existingPos = getRecordPositionInPage(selectedPage, positionInPage);
        if (existingPos != 0)
          throw new DatabaseOperationException(
              "Cannot restore record at position " + position + " in bucket '" + componentName
                  + "': the slot is occupied by a live record");
      }

      return selectedPage;

    } catch (final IOException e) {
      throw new DatabaseOperationException(
          "Cannot restore record at position " + position + " in bucket '" + componentName + "'", e);
    }
  }

  /**
   * Computes the content-byte offset where a new record's bytes should start within {@code page}, given the
   * page's CURRENT record count - i.e. right after the last live record/placeholder/chunk head on the page.
   * Mirrors the core of {@link #getFreeSpaceInPage}, minus the free-slot search (the caller already has an exact
   * target slot) and minus its {@code totalRecordsInPage >= maxRecordsInPage} short-circuit: that short-circuit
   * exists only to make the ALLOCATOR give up on this page and try a different one, which is not an option for
   * {@link #restoreRecordAtPosition} - the target page is fixed.
   */
  private int findContentInsertionOffset(final BasePage page, final int totalRecordsInPage) throws IOException {
    return contentEndOffset(page, getLastRecordPositionInPage(page, totalRecordsInPage));
  }

  /**
   * Content-byte offset right past the record whose bytes end last in the page, i.e. where the next record's bytes
   * can start. {@code lastRecordPositionInPage} is what {@link #getLastRecordPositionInPage} returned, so -1 means
   * the page holds no live slot at all and the content starts right after the header.
   * <p>
   * #6096: this used to be duplicated between the allocator ({@link #getFreeSpaceInPage}) and
   * {@link #findContentInsertionOffset}, and the two copies had already drifted - the restore-side copy took the
   * max over the record table without skipping the 0 entries a delete leaves behind, so on a page whose every slot
   * was a hole it read the entry as a record starting at content offset 0 (the record-count header) and placed the
   * restored record inside the page header, corrupting the slot table it had just written. Both callers now share
   * the one implementation.
   */
  private int contentEndOffset(final BasePage page, final int lastRecordPositionInPage) throws IOException {
    if (lastRecordPositionInPage == -1)
      // EMPTY PAGE (OR EVERY SLOT IS A HOLE): START RIGHT AFTER THE HEADER
      return contentHeaderSize;

    if (lastRecordPositionInPage < contentHeaderSize)
      // A LIVE SLOT POINTING INSIDE THE PAGE HEADER: THERE IS NO RECORD THERE, THE RECORD TABLE IS CORRUPTED. FAIL
      // LOUDLY INSTEAD OF DERIVING AN INSERTION OFFSET FROM HEADER BYTES AND OVERWRITING THE SLOT TABLE (#6096).
      // Unreachable through today's two callers - both derive the argument from getLastRecordPositionInPage(),
      // whose getRecordPositionInPage() already rejects a non-zero entry below contentHeaderSize with an
      // IOException. It is kept because this method is what a third caller would reuse, and a caller deriving the
      // offset its own way is precisely how #6096 happened: guarding here makes contentEndOffset() safe to reuse
      // on its own terms rather than only in the company of the current two. Mirrors getPageOccupiedInBytes().
      throw new DatabaseOperationException(
          "Invalid record position " + lastRecordPositionInPage + " in page " + page.getPageId() + " of bucket '" + componentName
              + "'");

    final long[] lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);
    if (lastRecordSize[0] == 0)
      // DELETED (V<24.1.1)
      return lastRecordPositionInPage + (int) lastRecordSize[1];
    else if (lastRecordSize[0] > 0)
      // RECORD PRESENT, CONSIDER THE RECORD SIZE + VARINT SIZE
      return lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
    else if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER)
      // PLACEHOLDER, CONSIDER NEXT 9 BYTES
      return lastRecordPositionInPage + LONG_SERIALIZED_SIZE + (int) lastRecordSize[1];
    else if (isChunkHead(lastRecordSize[0]) || lastRecordSize[0] == NEXT_CHUNK) {
      // CHUNK (of a record, or of a placeholder's content - the footprint is the same either way)
      final int chunkSize = page.readInt((int) (lastRecordPositionInPage + lastRecordSize[1]));
      return lastRecordPositionInPage + chunkFootprint((int) lastRecordSize[1], chunkSize);
    } else
      // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
      return lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];
  }

  /**
   * Reserves, for the current transaction, the slot the record being created will occupy on the REUSED page the
   * allocator picked (issue #5279), and registers the reservation so it is given back when the transaction ends.
   *
   * @return the slot to use, or -1 when the page cannot host the record once the in-flight reservations of the
   * other transactions are accounted for - the caller must then allocate a new page.
   */
  private int claimInsertSlot(final BasePage foundPage, final PageAnalysis pageAnalysis, final int candidateSlot,
                              final int candidateOffset, final int spaceNeeded) {
    final int reusedPageNumber = foundPage.getPageId().getPageNumber();
    final TransactionContext tx = database.getTransaction();
    // A slot BELOW the record count is a hole left by a delete. The page image the allocator walked may be this
    // transaction's own - dirty, and therefore older than the committed page - so on a page somebody ELSE is
    // inserting into, the hole is confirmed against the committed page with a single O(1) record-table read. On an
    // uncontended page the caller's image is the truth and the extra lookup would just cost a page read per insert.
    final boolean holeCandidate = candidateSlot < pageAnalysis.totalRecordsInPage;
    final PageInsertReservation probe = insertReservations.get(reusedPageNumber);
    final boolean holeVerified = holeCandidate && probe != null && probe.soleOwner != tx//
            && isSlotFreeInCommittedPage(reusedPageNumber, candidateSlot);
    final int reservedSlot = reserveInsertSlot(tx, reusedPageNumber, holeCandidate ? candidateSlot : -1, holeVerified,
            pageAnalysis.totalRecordsInPage, candidateOffset, spaceNeeded, foundPage.getMaxContentSize());
    if (reservedSlot >= 0)
      tx.trackInsertSlotReservation(this, reusedPageNumber, reservedSlot);
    return reservedSlot;
  }

  /**
   * Claims, for the calling transaction, the slot a brand-new record will occupy on an EXISTING page of this bucket
   * (issue #5279). Concurrent transactions inserting into the same page used to be handed the very same free slot -
   * and therefore the very same optimistic RID - because each of them picked it from its own snapshot of the page,
   * with nothing to make the choices disjoint. Every commit but the first then failed with a page-level
   * {@link ConcurrentModificationException} that no application-level retry could avoid, although two inserts of
   * DIFFERENT records always commute.
   * <p>
   * All the claims on one page are serialised on its reservation entry. The claim hands out:
   * <ul>
   * <li>a slot: {@code holeSlot} when the caller offers a hole (a slot freed by a delete) nobody else reserved -
   * on a shared page only if the caller could also verify it is still a hole on the COMMITTED page, since the
   * caller's own image of a page it already modified can be older than that; otherwise the first slot at or after
   * BOTH the caller's append position and the page high-water mark;</li>
   * <li>internally, a content offset - never returned, only used to decide whether the record still FITS once the
   * space promised to the other in-flight transactions is accounted for. The record itself is written at the
   * caller's own offset (its page image is private until commit) and, on a commit-time page conflict, the
   * disjoint-slot merge recomputes the offset on the newer committed page.</li>
   * </ul>
   * Both are derived with a {@code max()} against the caller's own snapshot, so a transaction inserting several
   * records into the same page is never charged twice for the records it already wrote there - and while the page
   * is uncontended the snapshot is used alone, since then it is the whole truth about the page.
   *
   * @return the reserved slot, or -1 when the page can no longer host the record and the caller must allocate a new
   * page.
   */
  private int reserveInsertSlot(final Object owner, final int pageNumber, final int holeSlot, final boolean holeVerified,
                                final int appendBase, final int candidateOffset, final int spaceNeeded,
                                final int maxContentSize) {
    for (;;) {
      PageInsertReservation res = insertReservations.get(pageNumber);
      if (res == null)
        res = insertReservations.computeIfAbsent(pageNumber, NEW_INSERT_RESERVATION);

      synchronized (res) {
        // The entry was dropped (its last reservation was released) between the lookup and the lock: it is no
        // longer the page's entry, so claiming on it would not be visible to anybody. Take a fresh one.
        if (res.removed)
          continue;
        if (res.outstanding == 0)
          res.soleOwner = owner;
        else if (res.soleOwner != owner)
          res.soleOwner = null;

        final boolean uncontended = res.soleOwner != null;

        final int slot;
        if (uncontended)
          slot = holeSlot >= 0 ? holeSlot : appendBase;
        else if (holeSlot >= 0 && holeVerified && !res.reservedSlots.get(holeSlot))
          slot = holeSlot;
        else {
          int candidate = Math.max(appendBase, res.nextFreeSlot);
          while (candidate < maxRecordsInPage && res.reservedSlots.get(candidate))
            ++candidate;
          slot = candidate;
        }

        final int offset = uncontended ? candidateOffset : Math.max(candidateOffset, res.nextFreeOffset);
        // Physical fit only, exactly like the page selection this claim refines: SPARE_SPACE_FOR_GROWTH is a
        // preference there, not a requirement (findAvailableSpace deliberately retries with half the size to
        // squeeze a record into the tail of an almost full page), so turning it into a hard limit here would
        // send every record landing in that tail to a brand-new page.
        if (slot >= maxRecordsInPage || offset + spaceNeeded > maxContentSize) {
          if (res.outstanding == 0) {
            res.removed = true;
            insertReservations.remove(pageNumber, res);
          }
          return -1;
        }

        res.reservedSlots.set(slot);
        if (slot >= res.nextFreeSlot)
          res.nextFreeSlot = slot + 1;
        if (uncontended || offset + spaceNeeded > res.nextFreeOffset)
          res.nextFreeOffset = offset + spaceNeeded;
        ++res.outstanding;
        return slot;
      }
    }
  }

  /**
   * Tells whether {@code slot} is still a hole on the CURRENT COMMITTED version of the page, bypassing the calling
   * transaction's own (possibly older) image of it. One record-table read, no scan: it is what makes hole reuse safe
   * for a transaction that already modified the page and would otherwise keep offering a slot a concurrent
   * transaction has committed into meanwhile (#5279). A page with no committed version yet (created by this
   * transaction) has no hole to confirm.
   */
  private boolean isSlotFreeInCommittedPage(final int pageNumber, final int slot) {
    try {
      final BasePage committed = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, false);
      if (committed == null)
        return false;
      return slot < committed.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET) && getRecordPositionInPage(committed, slot) == 0;
    } catch (final Exception e) {
      // Not being able to confirm the hole (page not on disk yet, unreadable, ...) is not an error: fall back to
      // appending, which is always safe.
      return false;
    }
  }

  /**
   * Tells whether a record is present at {@code rid} in the CURRENT COMMITTED state of its page, ignoring the
   * calling transaction's own (possibly older) image of that page. Only the record table is consulted - no content
   * is loaded - so it answers "is this slot taken right now?" at the cost of one page lookup.
   * <p>
   * Used by the unique-index check to tell a genuinely dangling entry (its record was deleted or its bucket is
   * gone) from one this transaction simply cannot see yet because a concurrent transaction inserted the record into
   * a page THIS transaction has also modified (#5279).
   */
  public boolean existsRecordInCommittedPage(final RID rid) {
    final long position = rid.getPosition();
    if (position < 0)
      return false;

    final int pageNumber = (int) (position / maxRecordsInPage);
    final int positionInPage = (int) (position % maxRecordsInPage);
    try {
      final BasePage committed = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, false);
      if (committed == null || positionInPage >= committed.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return false;

      final int recordPositionInPage = getRecordPositionInPage(committed, positionInPage);
      if (recordPositionInPage == 0)
        // FREE SLOT (NEVER USED OR DELETED)
        return false;

      // A ZERO SIZE MARKS A RECORD DELETED BEFORE v24.1.1; ANY OTHER VALUE (INCLUDING PLACEHOLDERS AND CHUNKS) IS A RECORD
      return committed.readNumberAndSize(recordPositionInPage)[0] != 0L;

    } catch (final Exception e) {
      // The page is not there (or not readable): the record is certainly not committed at that position.
      return false;
    }
  }

  /**
   * Releases one slot claimed by {@link #reserveInsertSlot}. Called for every reservation when the owning
   * transaction ends, whatever the outcome (commit, rollback or kill), from {@code TransactionContext.reset()}.
   */
  public void releaseInsertSlot(final int pageNumber, final int slot) {
    final PageInsertReservation res = insertReservations.get(pageNumber);
    if (res == null)
      return;
    synchronized (res) {
      res.reservedSlots.clear(slot);
      if (--res.outstanding <= 0) {
        res.removed = true;
        insertReservations.remove(pageNumber, res);
      }
    }
  }

  /**
   * A record UPDATE is a disjoint-slot-merge candidate unless it is an edge-list segment (record type 3): appending
   * to an existing chunk is owned by the commutative edge-append merge ({@link TransactionContext#trackEdgeAppend}),
   * which also handles the case the slot merge cannot - two transactions appending to the SAME chunk - so the two
   * mechanisms are kept from rebasing the same page. Creating a chunk is a different matter: see the caller in
   * updateRecordInternal and {@link TransactionContext#isSlotTrackedAsInsert} (#5279).
   */
  private static boolean isSlotMergeCandidate(final Record record) {
    return record.getRecordType() != EdgeSegment.RECORD_TYPE;
  }

  /**
   * Commit-time primitive for the disjoint-slot page merge (#5381). Re-applies ONE record write this transaction
   * made to a bucket page, on top of {@code page} freshly reloaded at its current committed version, keeping the
   * record's RID (page+slot) fixed. Called only on the leader/embedded commit while the bucket file's commit
   * lock is held, and only for a page whose every modification this transaction made was a tracked disjoint-slot
   * insert or an update that stayed inside the page (see {@code TransactionContext.rebaseSlots}).
   *
   * @param page           the reloaded committed page to re-apply the write onto.
   * @param positionInPage the record slot (RID position modulo maxRecordsInPage).
   * @param body           this transaction's final image of the slot: the serialized record body (no size prefix)
   *                       for a plain record or a placeholder content, the chunk header + content for a chunk head.
   * @param baseBody       for an UPDATE, the image the slot held when this transaction started - used to detect a
   *                       concurrent modification of the SAME record (a TRUE conflict); {@code null} for an INSERT.
   * @param kind           which shape the two images describe (see {@code TransactionContext.SLOT_KIND_*}). The
   *                       committed marker must still agree with it: the same bytes mean a different record behind a
   *                       different marker, so a slot whose shape changed under us is a conflict, never a merge.
   *
   * @return true when the write was safely re-applied; false when a concurrent commit took/changed the slot or the
   * page can no longer host the record - the caller then falls back to a full-transaction retry.
   */
  public boolean rebaseRecordOnPage(final MutablePage page, final int positionInPage, final byte[] body, final byte[] baseBody,
                                    final byte kind) {
    try {
      final int pageNumber = page.getPageId().getPageNumber();
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      final int existingPos = positionInPage < recordCountInPage ? getRecordPositionInPage(page, positionInPage) : 0;

      if (baseBody == null) {
        // INSERT: the slot must still be free on the committed page (else a concurrent commit used it).
        if (existingPos != 0)
          return false;
        return writeRecordAtSlot(page, pageNumber, positionInPage, recordCountInPage, body);
      }

      // UPDATE: the slot must still hold the record this transaction started from, byte-for-byte, so we never
      // overwrite (lose) a concurrent update of the SAME record.
      if (existingPos == 0)
        return false;
      final long[] rs = page.readNumberAndSize(existingPos);

      if (kind == TransactionContext.SLOT_KIND_FIRST_CHUNK
              || kind == TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT) {
        // #6129: the head chunk of a multi-page record. It keeps its slot and updateMultiPageRecord only rewrites
        // INSIDE the region that slot owns (the chunk size, the pointer to the next chunk and the chunk content), so
        // re-applying our final image at the same offset reproduces the page byte for byte - exactly as for a
        // same-or-smaller overwrite. What this check cannot see is the rest of the record, which lives on other
        // pages: that is covered before the write, by the chunk-chain fingerprint (see updateRecordInternal).
        // #6196: the head of a placeholder's CONTENT is the same shape behind a different marker, and the kind says
        // which one the write started from - a slot that changed from one to the other meanwhile is a conflict.
        if (rs[0] != (kind == TransactionContext.SLOT_KIND_FIRST_CHUNK ? FIRST_CHUNK : FIRST_CHUNK_PLACEHOLDER_CONTENT))
          return false;
        final int chunkHeaderPos = (int) (existingPos + rs[1]);
        final byte[] committedChunk = readChunkImage(page, chunkHeaderPos);
        if (committedChunk == null || !Arrays.equals(committedChunk, baseBody))
          return false;
        // #6163: the declared size may have grown back into the room the region still had. The write sized it
        // against the region of the page IT saw, so the one thing this replay may not do is take that on trust: it
        // re-derives the region from the COMMITTED page, exactly as growRecordInPage re-derives the free tail for a
        // plain record that grew. A concurrent commit that has taken those bytes - an insert into the tail a shrink
        // released - leaves a shorter region and sends this transaction to a normal retry, rather than writing our
        // longer head chunk straight over that record. A same-or-shorter image passes it by construction.
        final int chunkRegionEnd = headChunkRegionEnd(page, recordCountInPage, existingPos);
        if (chunkHeaderPos + body.length > chunkRegionEnd)
          return false;

        // #6154: same accounting the write makes, re-derived here for the same reason as the region - the write made
        // it against a page this one has moved on from. It is an absolute value, not an increment, so re-stating it
        // corrects the earlier one rather than doubling it.
        if (chunkRegionEnd == page.getMaxContentSize() && body.length != committedChunk.length)
          updatePageStatistics(page, chunkRegionEnd - (chunkHeaderPos + committedChunk.length),
                  committedChunk.length - body.length);

        page.writeByteArray(chunkHeaderPos, body, 0, body.length);
        return true;
      }

      if (kind == TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD
              || kind == TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT) {
        // #6178: the record shrank back inside its own slot and stopped being a chunk chain. The pre-image is a head
        // chunk, so it is checked as one - and as for every other head-chunk replay, the rest of the chain is covered
        // by the chain fingerprint the write compared before collapsing, not by anything visible here.
        // #6286: the CONTENT record of a placeholder collapses the same way into the negated shape of its own, and the
        // kind says which of the two the write started from - the marker it must still find, and the sign it writes.
        final boolean toPlaceholderContent = kind == TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT;
        if (rs[0] != (toPlaceholderContent ? FIRST_CHUNK_PLACEHOLDER_CONTENT : FIRST_CHUNK))
          return false;
        final int chunkHeaderPos = (int) (existingPos + rs[1]);
        final byte[] committedChunk = readChunkImage(page, chunkHeaderPos);
        if (committedChunk == null || !Arrays.equals(committedChunk, baseBody))
          return false;

        // The room is re-derived from the COMMITTED page for the same reason the head-chunk replay re-derives it
        // (#6163): the write measured it against a page this one has moved on from, and a concurrent commit is
        // allowed to have taken the bytes a shrink released. A region that can no longer host the plain record sends
        // the transaction to a normal retry instead of writing over whatever took them.
        //
        // No test reaches this refusal, and that is a statement about the engine rather than about the tests: a
        // region only shrinks when something is committed between this slot and its neighbour, and the room next to
        // a chunk head is released by the very transaction being replayed here - a collapse - which cannot have
        // committed yet. What could still do it is a compressPage that re-flows the page under us. So it stays, as
        // the head-chunk branch's own refusal above does, and neither is reachable by fabricating a page state
        // without testing the fabrication instead of the engine.
        final int chunkRegionEnd = headChunkRegionEnd(page, recordCountInPage, existingPos);
        final long sizeMarker = toPlaceholderContent ? -1L * body.length : body.length;
        final int sizeLen = Binary.getNumberSpace(sizeMarker);
        if (existingPos + sizeLen + body.length > chunkRegionEnd)
          return false;

        // #6154: same accounting the write makes, re-derived here against the committed page rather than trusted.
        if (chunkRegionEnd == page.getMaxContentSize()) {
          final int committedChunkEnd = chunkHeaderPos + committedChunk.length;
          updatePageStatistics(page, chunkRegionEnd - committedChunkEnd,
                  committedChunkEnd - (existingPos + sizeLen + body.length));
        }

        final int sizeLenWritten = page.writeNumber(existingPos, sizeMarker);
        assert sizeLenWritten == sizeLen :
            "the record size took " + sizeLenWritten + " bytes where " + sizeLen + " were budgeted";
        page.writeByteArray(existingPos + sizeLenWritten, body, 0, body.length);
        return true;
      }

      if (kind == TransactionContext.SLOT_KIND_RECORD_SPILLED_TO_CHUNK) {
        // #6129: the record outgrew its page and became a chunk HEAD without leaving its slot. The pre-image is still
        // a plain record, so it is checked as such; what has to be re-established is the room the spill used, which
        // is the record's own footprint plus - only when it is the last record - the free tail of the page. A
        // concurrent commit that appended after it, or that made the record any different, sends us back to a retry.
        if (rs[0] <= 0 || (int) rs[0] != baseBody.length)
          return false;
        if (!isCommittedRecordEqual(page, existingPos, rs, baseBody))
          return false;

        final int slotFootprint = (int) (rs[0] + rs[1]);
        int roomForTheChunk = slotFootprint;
        final int lastRecordPositionInPage = getLastRecordPositionInPage(page, recordCountInPage);
        final int pageOccupiedInBytes = getPageOccupiedInBytes(page, lastRecordPositionInPage, existingPos, rs);
        final int freeTailInPage = page.getMaxContentSize() - pageOccupiedInBytes;
        if (lastRecordPositionInPage == existingPos)
          roomForTheChunk += freeTailInPage;

        // The REAL marker cost, not the 2 bytes MINIMUM_SPACE_FOR_FIRST_CHUNK budgets for it - and the two agree,
        // because both sides size the whole footprint rather than the marker alone (see that constant's javadoc).
        final int markerSize = Binary.getNumberSpace(FIRST_CHUNK);
        // #6149: the write may have reached the chunk-header minimum by enlarging the slot through the in-page
        // shift. Re-do exactly that here - the records that follow merely move, and their offsets are recomputed
        // from THIS page, like the growth replay above. The decision runs through the SAME
        // slotEnlargementForChunkHead the write used, so the two can only differ in what they feed it: the slot
        // footprint is pinned by the pre-image just checked, while the free tail is not, and a committed page that
        // can no longer spare it sends the transaction to a normal retry.
        final int enlargement = slotEnlargementForChunkHead(markerSize + body.length, roomForTheChunk, freeTailInPage,
                lastRecordPositionInPage == existingPos);
        if (enlargement < 0)
          return false;
        if (enlargement > 0) {
          shiftFollowingRecordsRight(page, recordCountInPage, existingPos + slotFootprint, pageOccupiedInBytes,
                  enlargement);
          updatePageStatistics(page, freeTailInPage, -enlargement);
        }

        // The check above budgeted the marker at getNumberSpace(); the write below is the only other place that
        // decides how many bytes it takes. They agree by construction (writeNumber writes exactly that many), and a
        // future divergence would place the body past the room just proved - a silent overwrite of the next record
        // rather than a refusal, which is precisely the failure this assertion exists to make loud under -ea.
        final int markerSizeWritten = page.writeNumber(existingPos, FIRST_CHUNK);
        assert markerSizeWritten == markerSize :
            "the chunk marker took " + markerSizeWritten + " bytes where " + markerSize + " were budgeted";

        page.writeByteArray(existingPos + markerSizeWritten, body, 0, body.length);

        // #6396: a slot that spilled as the LAST record of its page eats into the free tail directly, and
        // slotEnlargementForChunkHead counts that tail as room the slot already has - so the enlargement above is 0
        // by construction there and reports nothing. How much of the tail the head chunk really took depends on the
        // COMMITTED page this replay landed on, not on the one the write measured, so it is measured here: the head
        // chunk ends where it ends, and everything past it is what the page has left.
        if (lastRecordPositionInPage == existingPos)
          updatePageStatistics(page, page.getMaxContentSize() - (existingPos + markerSizeWritten + body.length), 0);

        return true;
      }

      final boolean isPlaceHolder = kind == TransactionContext.SLOT_KIND_PLACEHOLDER_CONTENT;
      if (isPlaceHolder ? rs[0] >= RECORD_PLACEHOLDER_CONTENT : rs[0] <= 0)
        // Deleted, or no longer the shape our images describe (a plain record where we tracked placeholder content,
        // a placeholder/chunk marker where we tracked a plain record).
        return false;

      // From here on rs[0] is the record's SIZE, as the update path does before growing a placeholder content
      // record: the marker keeps its sign, only the size read out of it is made positive.
      if (isPlaceHolder)
        rs[0] *= -1L;

      final int committedSize = (int) rs[0];
      if (committedSize != baseBody.length)
        return false;
      if (!isCommittedRecordEqual(page, existingPos, rs, baseBody))
        // The committed record differs from our base: a concurrent transaction changed THIS record -> real conflict.
        return false;

      if (body.length > committedSize)
        // GROWTH (#5279): re-do the in-page growth on the committed page. The record keeps its slot (hence its RID)
        // and only its own content changes: the records that follow simply move, and their offsets are recomputed
        // from THIS page, so the result is the same page a serial execution would have produced. Refuses (false) when
        // the page cannot host the extra bytes anymore, which sends the transaction to a normal retry.
        return growRecordInPage(page, recordCountInPage, existingPos,
                getLastRecordPositionInPage(page, recordCountInPage), rs, isPlaceHolder, body, 0, body.length);

      final int sizeLen = page.writeNumber(existingPos, isPlaceHolder ? -1L * body.length : body.length);
      page.writeByteArray((int) (existingPos + sizeLen), body, 0, body.length);

      // #6396: the mirror of updateRecordInternal's same-or-shorter branch, and it gives bytes back the same way -
      // silently, because only the page's LAST record moves the free tail and nothing here knows whether this is it.
      // Saying so matters MORE on this path than on the live one: rebaseSlots replays every buffered slot onto ONE
      // page image and compresses it once at the end, so a shrink that stayed quiet would leave the exact claim
      // ANOTHER slot's replay made (a growth re-flowing the same page) standing over a page it no longer describes.
      if (sizeLen + body.length < rs[1] + committedSize)
        freedWithoutClaiming(page);

      return true;

    } catch (final IOException e) {
      // Intentional asymmetry: a "cannot rebase this slot" outcome returns false (the caller raises a clean CME
      // and the transaction retries), but a genuine I/O failure reading the page is not a retryable conflict - it
      // aborts the transaction like any other storage error rather than masquerading as a version conflict.
      throw new DatabaseOperationException("Error on slot rebase for page " + page.getPageId(), e);
    }
  }

  /**
   * Whether the record stored at {@code recordPositionInPage} holds exactly {@code baseBody}: the byte-for-byte
   * pre-image check every slot-merge replay makes before touching a slot, which is what tells a false page conflict
   * (a concurrent write to ANOTHER slot) from a true one (a concurrent write to THIS record).
   *
   * @param recordSize the {size, sizeLength} pair of the stored record, with the size already made positive.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static boolean isCommittedRecordEqual(final BasePage page, final int recordPositionInPage, final long[] recordSize,
                                                final byte[] baseBody) throws IOException {
    final byte[] committed = new byte[(int) recordSize[0]];
    page.readByteArray((int) (recordPositionInPage + recordSize[1]), committed, 0, committed.length);
    return Arrays.equals(committed, baseBody);
  }

  /**
   * The bytes a chunk record occupies on its page AFTER the marker: {@code [int chunkSize][long nextChunk][chunkSize
   * bytes of content]}. This is the image the disjoint-slot merge keeps for the head chunk of a multi-page record
   * (#6129) - the marker itself is excluded because it never changes while the slot stays a chunk head, and it is
   * verified separately against the committed page.
   *
   * @param chunkHeaderPos offset of the chunk size field, i.e. right after the marker.
   *
   * @return the image, or {@code null} when the header does not describe a chunk that fits the page (a corrupted or
   * concurrently rewritten slot), which the callers turn into a refusal to merge.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static byte[] readChunkImage(final BasePage page, final int chunkHeaderPos) throws IOException {
    final int chunkSize = page.readInt(chunkHeaderPos);
    final int imageSize = INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + chunkSize;
    if (chunkSize < 0 || chunkHeaderPos + imageSize > page.getMaxContentSize())
      return null;
    final byte[] image = new byte[imageSize];
    page.readByteArray(chunkHeaderPos, image, 0, imageSize);
    return image;
  }

  /**
   * Commit-time primitive for the disjoint-slot page merge (#5569). Re-applies the DELETE of one plain in-place
   * record on top of {@code page} freshly reloaded at its current committed version. Called only on the
   * leader/embedded commit while the bucket file's commit lock is held, and only for a page whose every modification
   * this transaction made was a tracked single-slot write (see {@code TransactionContext.rebaseSlots}).
   *
   * @param page           the reloaded committed page to re-apply the delete onto.
   * @param positionInPage the record slot (RID position modulo maxRecordsInPage).
   * @param baseBody       the record body this transaction started from - used to detect a concurrent modification
   *                       of the SAME record (a TRUE conflict).
   *
   * @return true when the delete was safely re-applied; false when the committed slot no longer holds the very same
   * record - the caller then falls back to a full-transaction retry.
   */
  public boolean rebaseRecordDeleteOnPage(final MutablePage page, final int positionInPage, final byte[] baseBody) {
    try {
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      final int existingPos = positionInPage < recordCountInPage ? getRecordPositionInPage(page, positionInPage) : 0;

      // The slot must still hold the record this transaction started from, byte-for-byte: a concurrent commit that
      // deleted or rewrote THIS record is a real conflict the application has to see.
      if (existingPos == 0 || baseBody == null)
        return false;
      final long[] rs = page.readNumberAndSize(existingPos);
      if (rs[0] <= 0)
        // Deleted, placeholder, or multi-page marker: not the plain in-place record we captured anymore.
        return false;
      final int committedSize = (int) rs[0];
      if (committedSize != baseBody.length)
        return false;
      final byte[] committed = new byte[committedSize];
      page.readByteArray((int) (existingPos + rs[1]), committed, 0, committedSize);
      if (!Arrays.equals(committed, baseBody))
        return false;

      if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE))
        // WIPE OUT RECORD CONTENT, EXACTLY AS deleteRecordInternal DOES
        page.writeZeros(existingPos + 1, (int) (committedSize + rs[1] - 1));

      // POINTER = 0 MEANS DELETED. The in-page record count is deliberately left alone, as on the normal delete
      // path: it is the size of the slot table, not the number of live records, and compressPage (which the commit
      // runs on the rebased page right after) shrinks it when the freed slots are the trailing ones.
      page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);

      if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.HIGH.ordinal()) {
        // #6358: the free tail is MEASURED here, on the page the slot has just been freed on, and reported as the
        // measurement it is - the delta convention of updatePageStatistics carries an absolute value as
        // (theSpaceThereIs, 0). Subtracting the record's footprint from it, as this did, is the same double count
        // #6339 removed from the ordinary delete: the measurement already accounts for whatever the free released,
        // because it walks the page AFTER the slot was zeroed. On a record freed in the MIDDLE of its page the tail
        // does not move at all, so the subtraction was pure loss and could take the number below zero.
        //
        // Kept rather than left to the compressPage the commit runs on this page a moment later (which measures the
        // same thing), because the walk is already done here and dropping the call would also drop the
        // changesFromLastStats increment that schedules the next full statistics gather.
        final PageAnalysis pageAnalysis = new PageAnalysis(page);
        pageAnalysis.totalRecordsInPage = recordCountInPage;
        getFreeSpaceInPage(pageAnalysis);
        updatePageStatistics(page, pageAnalysis.spaceAvailableInCurrentPage, 0);
      } else
        changesFromLastStats.incrementAndGet();

      return true;

    } catch (final IOException e) {
      // Same asymmetry as rebaseRecordOnPage: a "cannot rebase this slot" outcome returns false, a genuine I/O
      // failure aborts the transaction instead of masquerading as a version conflict.
      throw new DatabaseOperationException("Error on slot delete rebase for page " + page.getPageId(), e);
    }
  }

  private boolean writeRecordAtSlot(final MutablePage page, final int pageNumber, final int positionInPage,
                                    final short recordCountInPage, final byte[] body) throws IOException {
    final int spaceNeeded = Binary.getNumberSpace(body.length) + body.length;

    // Find where free content begins on the CURRENT committed page (reuses the tested free-space walker). Only the
    // HARD verdicts are honoured here: an unusable slot table or a page with no room left leaves
    // newRecordPositionInPage negative, while a plain createNewPage also fires on the SOFT
    // SPARE_SPACE_FOR_GROWTH preference - which would refuse to replay a record the allocator had deliberately
    // squeezed into the tail of an almost full page (#5279).
    final PageAnalysis analysis = getAvailableSpaceInPage(pageNumber, spaceNeeded, false);
    if (analysis.newRecordPositionInPage < 0//
            || spaceNeeded > page.getMaxContentSize() - analysis.newRecordPositionInPage)
      // The page filled up under concurrency: fall back to a full retry (which will pick a new page/slot).
      return false;

    final int contentPos = analysis.newRecordPositionInPage;

    page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, contentPos);
    if (positionInPage + 1 > recordCountInPage)
      page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (positionInPage + 1));

    final int sizeLen = page.writeNumber(contentPos, body.length);
    page.writeByteArray(contentPos + sizeLen, body, 0, body.length);
    updatePageStatistics(page, page.getMaxContentSize() - contentPos, -spaceNeeded);
    return true;
  }

  /**
   * Creates multiple records from pre-serialized Binary buffers in a single operation.
   * Always appends to new pages to avoid complexity with existing page layouts, deletions,
   * and multi-page records. This avoids the per-record findAvailableSpace overhead and is
   * designed for bulk import scenarios where sequential page writes are optimal.
   *
   * @param buffers    array of pre-serialized record content (NOT including the size prefix)
   * @param from       start index in buffers array (inclusive)
   * @param to         end index in buffers array (exclusive)
   * @param ridsOut    output array for assigned RIDs (must have length >= to - from)
   */
  public void createRecordsBulk(final Binary[] buffers, final int from, final int to, final RID[] ridsOut) {
      // Enforce the per-type CREATE_RECORD ACL once for the whole bulk (a no-op when no principal is bound,
      // e.g. internal import or HA apply). The single-record createRecord() path checks this per record; the
      // bulk edge path (GraphBatch flush) previously skipped it, so an authenticated user with no createRecord
      // grant on this type could still write edges here once the request thread bound the principal
      // (defense-in-depth for GHSA-c23x-pqcj-7hfm).
      database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

      // Always start from a fresh new page for bulk writes.
      // Atomically reserve the next page number to prevent two concurrent transactions
      // from allocating the same page number (which would cause silent data corruption).
      MutablePage currentPage = database.getTransaction()
          .addPage(new PageId(database, file.getFileId(), reservedPageCounter.getAndIncrement()), pageSize);
      int recordPositionInPage = contentHeaderSize;
      int availablePositionIndex = 0;
      final int maxContent = currentPage.getMaxContentSize();

      for (int i = from; i < to; i++) {
        final Binary buffer = buffers[i];

        // Pad to minimum record size
        while (buffer.size() < MINIMUM_RECORD_SIZE)
          buffer.append((byte) 0);

        final int actualSize = buffer.size();
        final int spaceNeeded = Binary.getNumberSpace(actualSize) + actualSize;

        // Check if current page has room
        if (availablePositionIndex >= maxRecordsInPage
            || recordPositionInPage + spaceNeeded > maxContent) {
          // Finalize current page record count
          currentPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) availablePositionIndex);

          // Create a new page
          currentPage = database.getTransaction()
              .addPage(new PageId(database, file.getFileId(), reservedPageCounter.getAndIncrement()), pageSize);
          recordPositionInPage = contentHeaderSize;
          availablePositionIndex = 0;
        }

        // Write the record offset in the record table
        currentPage.writeUnsignedInt(
            PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
            recordPositionInPage);

        // Write record size + content
        final int sizeBytes = currentPage.writeNumber(recordPositionInPage, actualSize);
        currentPage.writeByteArray(recordPositionInPage + sizeBytes,
            buffer.getContent(), buffer.getContentBeginOffset(), actualSize);

        // Assign RID
        ridsOut[i - from] = new RID(file.getFileId(),
            ((long) currentPage.getPageId().getPageNumber()) * maxRecordsInPage + availablePositionIndex);

        recordPositionInPage += sizeBytes + actualSize;
        availablePositionIndex++;
      }

    // Finalize last page record count
    if (availablePositionIndex > 0)
      currentPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) availablePositionIndex);
  }

  private boolean updateRecordInternal(final Record record, final RID rid, final boolean updatePlaceholderContent,
                                       final boolean discardRecordAfter) {
    if (rid.getPosition() < 0)
      throw new IllegalArgumentException("Cannot update a record with invalid RID");

    final Binary buffer = database.getSerializer().serialize(database, record);

    // A CONTENT record stores its size NEGATED, so a body shorter than MINIMUM_RECORD_SIZE would write a -1..-4 that
    // the marker namespace already owns, and one of exactly MINIMUM_RECORD_SIZE would write the -5 BOUNDARY every
    // reader excludes with a strict `<` (see that constant). Pad past both, exactly as createRecordInternal pads the
    // record it creates behind a new pointer. It matters on every write that gives such a slot its size back - the
    // in-place overwrite far below, and since #6286 the collapse of a content chain into this very shape.
    if (updatePlaceholderContent)
      while (buffer.size() <= MINIMUM_RECORD_SIZE)
        buffer.append((byte) 0);

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
//          .log(this, Level.SEVERE, "UPDATE %s pageV=%d content %s (threadId=%d)", rid, page.getVersion(), record.toJSON(), Thread.currentThread().getId());

      // DISJOINT-SLOT MERGE (#5381, #5279): only an update that stays INSIDE this page - an overwrite of the same
      // size or smaller, or a growth the page can host by shifting the records that follow (the two branches far
      // below) - touches just this record's slot and thus commutes with concurrent writes to other slots on the
      // page. Every other update shape here (placeholder pointer or content, multi-page chunk, a record that has to
      // spill OUT of the page) changes more than this slot, so it poisons the page: the merge must never rebase it.
      final TransactionContext slotTx = database.getTransactionIfExists();
      final boolean slotMergeOn = slotTx != null && slotTx.isSlotMergeEnabled();
      // #5279: an edge-list segment is normally left to the edge-append merge, but a segment this transaction
      // CREATED (its slot is already tracked as an insert) has no committed image to append onto - the edge merge
      // cannot replay it and deliberately poisoned its page. The slot merge owns it instead: the whole chunk
      // content is ours, so re-applying the final image at the same free slot reproduces it exactly.
      final boolean slotInsertedHere = slotMergeOn && slotTx.isSlotTrackedAsInsert(fileId, pageId, positionInPage);
      final boolean slotCandidate = slotMergeOn && (isSlotMergeCandidate(record) || slotInsertedHere);
      // A NON-candidate update (an edge-list segment, owned by the edge-append merge) still modifies this page:
      // it must POISON the slot map, not stay invisible to it. Since super-node striping (#5156) a segments page
      // can also host a StripeDirectory - a slot-merge candidate - and a page carrying a tracked directory write
      // plus an untracked segment change would otherwise be slot-rebased from the directory write alone, silently
      // dropping this transaction's in-chunk appends. Mirrors the !isSlotMergeCandidate poison in createRecord.
      if (slotMergeOn && !slotCandidate)
        slotTx.poisonSlotRebasePage(fileId, pageId);

      // #5596: an in-chunk edge append is invisible to the slot map on purpose - the commutative edge-append merge
      // owns it - so the in-place rewrite of the segment below is declared covered by THAT merge instead. Only when
      // the append was really registered (TransactionContext.trackEdgeAppend) and the page is not already
      // excluded: a segment write nobody tracked (an edge removal, addAll, a bulk load) stays undeclared and can
      // therefore never be rebased away, whether or not its writer remembered to poison the page.
      final boolean edgeAppendReplayable = !slotCandidate && slotTx != null && slotTx.isEdgeAppendMergeEnabled()//
              && slotTx.isEdgeAppendTracked(rid) && !slotTx.isEdgeAppendPagePoisoned(fileId, pageId);

      boolean isPlaceHolder = false;
      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {

        // FOUND A RECORD POINTED FROM A PLACEHOLDER
        final RID placeHolderContentRID = new RID(fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));

        // #6141: this slot holds 8 bytes of pointer and the record's WHOLE content lives on another page - a page
        // this transaction pinned nothing on, and which the write below therefore reaches at its newest committed
        // version, under the commit lock. Neither the version check nor any pre-image check can see a concurrent
        // update of this record: both writers read the content page fresh and the second one used to win silently.
        // Comparing the content against the fingerprint taken when the record was taken for update
        // (TransactionContext.addUpdatedRecord) is what makes that conflict visible, and it is the only thing that
        // can - so this check is NOT conditional on any merge being enabled. Absent fingerprint = this update never
        // went through addUpdatedRecord (a commit-time replay), and there is no "before" to compare against.
        final Long capturedContent = slotTx == null ? null : slotTx.getOffPageFingerprint(rid);
        if (capturedContent != null && capturedContent != placeholderContentFingerprint(placeHolderContentRID))
          throw new ConcurrentModificationException(
                  "Record " + rid + " was modified by a concurrent transaction. Please retry the operation");

        if (updateRecordInternal(record, placeHolderContentRID, true, discardRecordAfter)) {
          // UPDATE PLACEHOLDER CONTENT, THE PLACEHOLDER POINTER STAY THE SAME. Nothing was written to THIS page, so
          // it is left exactly as this transaction found it - no poisoning (#6129): the content record's page is the
          // one that changed, and the nested call tracked or poisoned that one on its own. Poisoning here used to
          // cost the merge of a page whose only real change was some OTHER record this transaction updated.
          if (!discardRecordAfter)
            ((RecordInternal) record).setBuffer(buffer.getNotReusable());
          return true;
        }

        // DELETE OLD PLACEHOLDER, A NEW PLACEHOLDER WILL BE CREATED WITH ENOUGH SPACE
        deleteRecordInternal(placeHolderContentRID, true, false, false);

        // The slot is being turned from a placeholder POINTER back into a record, and the content record it pointed
        // to (on another page) has just been deleted: this is not a single-slot change, and the "pre-image" the
        // branches below would capture is the 8-byte pointer, not record content. Poison the page so no merge can
        // ever rebase this slot from it.
        if (slotCandidate)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        recordSize[0] = LONG_SERIALIZED_SIZE;
        recordSize[1] = 1L;
      } else if (isChunkHead(recordSize[0])) {
        // #6196: a head chunk of either kind - a record of its own, or the CONTENT of a placeholder that no page could
        // host whole. Everything below treats the two identically (the chain, its region and its footprint are the
        // same) except the two places the difference is the whole point: the un-spill further down, which would have
        // to write back a marker this branch is not equipped to write, and the slot kind the merge is told.
        final boolean placeholderContentHead = recordSize[0] == FIRST_CHUNK_PLACEHOLDER_CONTENT;
        if (placeholderContentHead && !updatePlaceholderContent)
          // A CONTENT RECORD IS NOT UPDATABLE ON ITS OWN, exactly as the negated-size shape below is not.
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        // DISJOINT-SLOT MERGE (#6129): the head chunk of a multi-page record. It keeps its slot and its region on
        // THIS page - updateMultiPageRecord rewrites the chunk size, the pointer to the next chunk and the chunk
        // content, all inside the bytes the region covers - so the change this page sees is a single-slot write that
        // commutes with writes to every other slot, exactly like an in-place overwrite. Before this, a bucket whose
        // records had all outgrown their page serialized every writer on the page holding their heads.
        // #6163: the declared size may go UP as well as down inside that region, which is why the replay re-derives
        // the region from the committed page rather than refusing every head chunk longer than the one it finds.
        final int chunkHeaderPos = (int) (recordPositionInPage + recordSize[1]);
        final int chunkRegionEnd = headChunkRegionEnd(page, recordCountInPage, recordPositionInPage);
        // A slot this transaction CREATED cannot be a chunk head (writeMultiPageRecord poisons the page it spills
        // on), so there is no insert variant to keep here: refuse to track and let the page fall back to a retry.
        //
        // The chain fingerprint is the other half of the pre-image check, and it is not optional: the images tracked
        // below describe the head chunk ONLY, while the rest of the record lives on pages this transaction reads
        // fresh under the commit lock - so the page-version check cannot see a concurrent write to them either. A
        // transaction that rewrote this record past its head chunk would therefore be replayed over and silently
        // dropped. Comparing the chain against the fingerprint taken when the record was taken for update
        // (TransactionContext.addUpdatedRecord) is what rules that out; when they differ this falls back to what
        // happened before #6129 - the page is poisoned, and the version conflict a concurrent write to the record
        // always raises on it (it rewrites the head chunk too) sends the transaction to a retry.
        final boolean chunkRebasable = slotCandidate && !slotInsertedHere//
                && !slotTx.isSlotRebasePagePoisoned(fileId, pageId)//
                && slotTx.isChunkChainTailUnchanged(rid, chunkChainTailFingerprint(page, chunkHeaderPos));
        final byte[] chunkBaseImage = chunkRebasable ? readChunkImage(page, chunkHeaderPos) : null;
        if (chunkBaseImage == null && slotMergeOn)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // #6178: THE RECORD HAS SHRUNK BACK INSIDE THE REGION ITS OWN SLOT OWNS: UN-SPILL IT.
        //
        // Nothing used to do this, so a record that had spilled once stayed a chunk chain for the rest of its life -
        // 13 bytes of header for ever, every read and write on the chunk path, and check() reporting a multi-page
        // record that is not one. It is the exact mirror of the spill (SLOT_KIND_RECORD_SPILLED_TO_CHUNK), bounded by
        // the same rule #6163 sizes the head chunk with: the region, and not a byte more - shrinking is no licence to
        // take a neighbour's bytes.
        //
        // #6286: the CONTENT record of a placeholder collapses too, into the NEGATED size marker that says whose the
        // slot is. Which sign to write is decided by the FLAG and not by the marker found, and it has to be: a
        // database written before #6196 holds a content head still wearing the ambiguous FIRST_CHUNK, which the marker
        // cannot tell from a record's own - and collapsing THAT with a positive marker would make the placeholder's
        // content show up a second time as a document in its own right, which is #6196 all over again on a record that
        // had escaped it. The collapse of such a legacy head is therefore right, and it even ends the ambiguity for
        // good; what it cannot be is REPLAYED, because the two collapse kinds each name the one committed marker the
        // replay may write over and neither of them is FIRST_CHUNK-into-content. So it is handed no pre-image, which
        // is how it is told not to track, and the page is poisoned instead - but only if the collapse REALLY happened.
        // A legacy content head that stays a chain is the shape it always was, replayable under SLOT_KIND_FIRST_CHUNK
        // by the tracking further down, and poisoning it up front would have cost it that for nothing (PR review).
        final boolean legacyAmbiguousContentHead = updatePlaceholderContent && !placeholderContentHead;

        if (collapseChunkChainToRecord(rid, buffer, page, pageId, positionInPage, recordPositionInPage, chunkHeaderPos,
                chunkRegionEnd, legacyAmbiguousContentHead ? null : chunkBaseImage, updatePlaceholderContent, slotTx)) {
          // The slot changed and no tracked image accounts for it: a page rebased from some OTHER slot's write would
          // re-derive this one from the committed image and quietly resurrect the chain.
          if (legacyAmbiguousContentHead && slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (!discardRecordAfter)
            ((RecordInternal) record).setBuffer(buffer.getNotReusable());
          return true;
        }

        // #5596: the chunk-header and chunk-content writes below are what rebaseRecordOnPage re-does on the newer
        // committed page. Only THIS page's writes are declared: updateMultiPageRecord leaves the ones it makes to
        // the other pages of the chain undeclared, and poisons them.
        updateMultiPageRecord(rid, buffer, page, chunkHeaderPos,
                chunkBaseImage != null ? MutablePage.COVERAGE_SLOT_MERGE : 0, chunkRegionEnd);

        // updateMultiPageRecord may have poisoned this very page meanwhile (a continuation chunk of the chain landed
        // on it, or a chunk it freed lives on it): re-check before trusting the images.
        if (chunkBaseImage != null && !slotTx.isSlotRebasePagePoisoned(fileId, pageId)) {
          final byte[] chunkFinalImage = readChunkImage(page, chunkHeaderPos);
          if (chunkFinalImage != null)
            slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, chunkBaseImage, chunkFinalImage,
                    placeholderContentHead ?
                            TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT :
                            TransactionContext.SLOT_KIND_FIRST_CHUNK);
          else
            // The header no longer describes a chunk that fits the page: nothing here can be replayed safely.
            slotTx.poisonSlotRebasePage(fileId, pageId);
        }

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

        // DISJOINT-SLOT MERGE (#5279): growing a record INSIDE its own page changes only this record's content - the
        // records that follow merely move, and their new offsets are recomputed from the page itself - so the write
        // is replayable on a newer committed version of the page exactly like a same-or-smaller overwrite. Growth is
        // the normal update shape (a longer string, one more property), so leaving it out made concurrent updates of
        // unrelated records on one page conflict for good. The pre-image must be copied BEFORE the shift overwrites
        // it, but the tracking itself happens only if the growth really fits: spilling the record out of the page
        // (placeholder pointer or multi-page chunks) changes more than this slot and poisons the page.
        // #6129: a PLACEHOLDER CONTENT record grows here too. It is the same single-slot write - the pointer that
        // references it lives on ANOTHER page and is not touched by a growth that stays on this one, and its RID
        // (page+slot) does not move - so it is tracked with its own kind, which is what makes the replay put the
        // negative size marker back. A slot this transaction INSERTED is never a placeholder content record
        // (createRecordInternal poisons the page for those), hence no insert variant below.
        final boolean growRebasable = slotCandidate && !(isPlaceHolder && slotInsertedHere)//
                && !slotTx.isSlotRebasePagePoisoned(fileId, pageId);
        final byte[] growBaseBody;
        if (growRebasable && !slotInsertedHere) {
          growBaseBody = new byte[(int) recordSize[0]];
          page.readByteArray((int) (recordPositionInPage + recordSize[1]), growBaseBody, 0, growBaseBody.length);
        } else
          growBaseBody = null;

        // #5596: the in-page growth (the shift of the following records plus their recomputed offsets) is exactly
        // what rebaseRecordOnPage re-does on the newer committed page, so those bytes are covered by the slot merge.
        // A growth that does NOT fit writes nothing here and falls through to the spill branch below, undeclared.
        final int growCoverage = page.beginCoveredWrite(growRebasable ?
                MutablePage.COVERAGE_SLOT_MERGE :
                (edgeAppendReplayable ? MutablePage.COVERAGE_EDGE_APPEND_MERGE : 0));
        final boolean grown;
        try {
          grown = growRecordInPage(page, recordCountInPage, recordPositionInPage, lastRecordPositionInPage, recordSize,
                  isPlaceHolder, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
        } finally {
          page.endCoveredWrite(growCoverage);
        }

        if (grown) {
          if (slotCandidate) {
            if (growRebasable) {
              final byte[] finalBody = Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(),
                      buffer.getContentBeginOffset() + bufferSize);
              if (growBaseBody != null)
                slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, growBaseBody, finalBody,
                        isPlaceHolder ? TransactionContext.SLOT_KIND_PLACEHOLDER_CONTENT : TransactionContext.SLOT_KIND_RECORD);
              else
                // The slot holds a record CREATED by this transaction: there is no committed pre-image to diff
                // against, so it stays an insert whose final image the merge re-writes at the same free slot.
                slotTx.trackRebasableInsert(fileId, pageId, positionInPage, finalBody);
            } else
              slotTx.poisonSlotRebasePage(fileId, pageId);
          }

          LogManager.instance()
                  .log(this, Level.FINE, "Updated record %s by allocating new space on the same page (%s threadId=%d)", null, rid, page,
                          Thread.currentThread().getId());

        } else {
          // THE RECORD MUST SPILL OUT OF THE PAGE.
          if (isPlaceHolder) {
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            if (slotCandidate)
              slotTx.poisonSlotRebasePage(fileId, pageId);
            return false;
          }

          final int pageOccupiedInBytes = getPageOccupiedInBytes(page, lastRecordPositionInPage, recordPositionInPage,
                  recordSize);
          final int slotFootprint = (int) (recordSize[0] + recordSize[1]);
          final int freeTailInPage = page.getMaxContentSize() - pageOccupiedInBytes;
          int availableSpaceInCurrentPage = slotFootprint;
          if (lastRecordPositionInPage == recordPositionInPage)
            // SINCE IT'S THE LAST RECORD IN THE PAGE, GET ALSO THE REST OF THE SPACE AVAILABLE IN THE PAGE
            availableSpaceInCurrentPage += freeTailInPage;

          // #6149: the slot is too small to host a chunk header, which is the ONLY thing that still makes a record
          // spill into a placeholder - the pre-chunk mechanism, the one shape the disjoint-slot merge cannot replay
          // (the pointer rewrite changes two pages at once) and the one behind the silent lost update of #6141.
          // Reaching this branch means the page could not host the record's FULL new size, which says nothing about
          // the handful of bytes that separate a 9-byte slot from a chunk header: ask for them through the same
          // in-page shift a growth uses. The decision itself lives in slotEnlargementForChunkHead, shared with the
          // commit-time replay so the two cannot drift apart.
          final int slotEnlargement = slotEnlargementForChunkHead(MINIMUM_SPACE_FOR_FIRST_CHUNK,
                  availableSpaceInCurrentPage, freeTailInPage, lastRecordPositionInPage == recordPositionInPage);
          if (slotEnlargement > 0)
            availableSpaceInCurrentPage += slotEnlargement;

          // TODO: LOOK FOR 1/2 OF THE RECORD SIZE
          if (availableSpaceInCurrentPage < MINIMUM_SPACE_FOR_FIRST_CHUNK) {
            // The slot becomes a placeholder POINTER to a record created on another page: two pages change together
            // and the slot stops holding record content at all, so no merge can replay it from this page alone.
            if (slotCandidate)
              slotTx.poisonSlotRebasePage(fileId, pageId);

            final int bytesWritten = page.writeNumber(recordPositionInPage, RECORD_PLACEHOLDER_POINTER);

            final RID realRID = createRecordInternal(record, true, false);
            page.writeLong(recordPositionInPage + bytesWritten, realRID.getPosition());

            LogManager.instance()
                    .log(this, Level.FINE, "Updated record %s by allocating new space with a placeholder (%s threadId=%d)", null, rid,
                            page, Thread.currentThread().getId());
          } else {
            // SPLIT THE RECORD IN CHUNKS AS LINKED LIST AND STORE THE FIRST PART ON CURRENT PAGE ISSUE https://github.com/ArcadeData/arcadedb/issues/332
            //
            // DISJOINT-SLOT MERGE (#6129): the record turns into a chunk HEAD without leaving its slot, and the bytes
            // written here never go past what the slot may use (its own footprint, plus the free tail of the page when
            // it is the last record). So this page's part of the spill is still a single-slot write - it just replaces
            // a plain record image with a chunk one, which is why it is tracked with its own kind. The continuation
            // chunks land on other pages, which writeMultiPageRecord poisons. Leaving this shape out was not a
            // one-off cost: a transaction whose spill loses the race retries and spills again, so on a contended page
            // the records that have to spill can starve instead of converging.
            final boolean spillRebasable = growBaseBody != null && !slotTx.isSlotRebasePagePoisoned(fileId, pageId);

            if (slotEnlargement > 0) {
              // #6149: make the slot reach the chunk-header minimum. Declared under the same coverage as the head
              // chunk itself: the replay of a spilled-to-chunk slot re-does this shift on the committed page.
              //
              // This span is deliberately CLOSED before writeMultiPageRecord opens its own, rather than left open
              // around both the way growRecordInPage wraps its shift and its write in one. The difference matters:
              // when the chunk chain comes back to this very page, writeMultiPageRecord writes a CONTINUATION chunk
              // onto it through inline record-table writes it does not wrap in a coverage span of their own - it
              // poisons the page instead. Those writes would inherit an outer span left open here and be declared
              // slot-merge replayable, which they are not. Do not merge the two spans.
              final int shiftCoverage = page.beginCoveredWrite(spillRebasable ? MutablePage.COVERAGE_SLOT_MERGE : 0);
              try {
                shiftFollowingRecordsRight(page, recordCountInPage, recordPositionInPage + slotFootprint,
                        pageOccupiedInBytes, slotEnlargement);
              } finally {
                page.endCoveredWrite(shiftCoverage);
              }
              updatePageStatistics(page, freeTailInPage, -slotEnlargement);
            }

            // #6154: the slot's own footprint and the bytes the shift just claimed are already accounted for; the
            // rest of what the head chunk is given - the free tail, when this is the page's last record - is not.
            // A placeholder CONTENT record never reaches here: the "CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER" branch
            // above returns first and has the caller delete and recreate it, which is where its chain is written.
            writeMultiPageRecord(rid, buffer, page, recordPositionInPage, availableSpaceInCurrentPage,
                    spillRebasable ? MutablePage.COVERAGE_SLOT_MERGE : 0,
                    slotFootprint + Math.max(slotEnlargement, 0), false);

            if (!spillRebasable) {
              if (slotMergeOn)
                slotTx.poisonSlotRebasePage(fileId, pageId);
            } else if (!slotTx.isSlotRebasePagePoisoned(fileId, pageId)) {
              // writeMultiPageRecord may have poisoned this very page meanwhile (a continuation chunk landed on it).
              final long[] chunkMarker = page.readNumberAndSize(recordPositionInPage);
              final byte[] spillImage = chunkMarker[0] == FIRST_CHUNK ?
                      readChunkImage(page, (int) (recordPositionInPage + chunkMarker[1])) :
                      null;
              if (spillImage != null)
                slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, growBaseBody, spillImage,
                        TransactionContext.SLOT_KIND_RECORD_SPILLED_TO_CHUNK);
              else
                slotTx.poisonSlotRebasePage(fileId, pageId);
            }

            LogManager.instance().log(this, Level.FINE,
                    "Updated record %s by splitting it in multiple chunks to be saved in multiple pages (%s threadId=%d)", null, rid,
                    page, Thread.currentThread().getId());
          }
        }
      } else {
        // UPDATED RECORD CONTENT IS NOT LARGER THAN PREVIOUS VERSION: OVERWRITE THE CONTENT
        // CREATE A HOLE (REMOVED LATER BY COMPRESS-PAGE)

        // DISJOINT-SLOT MERGE (#5381): a plain in-place overwrite of a single record - the rebasable case (e.g.
        // the vertex edge-list head-pointer flip on super-node insertion). Capture the pre-image BEFORE writing:
        // at commit it lets the rebase tell a false page conflict (concurrent write to ANOTHER slot) from a true
        // one (a concurrent write to THIS record). #6129: placeholder CONTENT is rebasable here too - the pointer
        // that references it is on another page and stays untouched - it is just tracked with its own kind so the
        // replay writes the negative size marker back.
        final boolean slotTracked = slotCandidate && !(isPlaceHolder && slotInsertedHere)//
                && !slotTx.isSlotRebasePagePoisoned(fileId, pageId);
        if (slotCandidate) {
          // Skip the pre-image + final-image copies on a page that is already poisoned (they would be discarded).
          if (slotTracked) {
            final byte[] finalBody = Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(),
                    buffer.getContentBeginOffset() + bufferSize);
            if (slotInsertedHere)
              // The slot holds a record CREATED by this transaction: it stays an insert (there is no committed
              // pre-image to diff against), so only refresh its final image - and skip copying one altogether.
              slotTx.trackRebasableInsert(fileId, pageId, positionInPage, finalBody);
            else {
              final byte[] baseBody = new byte[(int) recordSize[0]];
              page.readByteArray((int) (recordPositionInPage + recordSize[1]), baseBody, 0, baseBody.length);
              slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, baseBody, finalBody,
                      isPlaceHolder ? TransactionContext.SLOT_KIND_PLACEHOLDER_CONTENT : TransactionContext.SLOT_KIND_RECORD);
            }
          } else
            slotTx.poisonSlotRebasePage(fileId, pageId);
        }

        // #5596: the overwrite of ONE record's size marker and content is what the slot merge replays (or, for a
        // tracked in-chunk edge append, what the edge-append merge re-derives). Declare exactly those bytes.
        final int previousCoverage = page.beginCoveredWrite(slotTracked ?
                MutablePage.COVERAGE_SLOT_MERGE :
                (edgeAppendReplayable ? MutablePage.COVERAGE_EDGE_APPEND_MERGE : 0));
        final long footprintBefore = recordSize[0] + recordSize[1];
        try {
          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
          page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
        } finally {
          page.endCoveredWrite(previousCoverage);
        }

        // #6396: an overwrite that is SHORTER than what the slot held gives those bytes back, and - like the delete,
        // and for the same reason (#6339) - says nothing about them: telling the statistics would cost the
        // slot-table scan this branch exists to avoid, since only the LAST record of a page moves its free tail and
        // nothing here knows whether this is it. The compression the commit runs measures the packed page anyway. A
        // same-size overwrite (the edge-append rewrite, a property changed in place) moves nothing and leaves the
        // claim exactly as it was.
        if (bufferSize + recordSize[1] < footprintBefore)
          freedWithoutClaiming(page);

        LogManager.instance()
                .log(this, Level.FINE, "Updated record %s with the same size or less as before (%s threadId=%d)", null, rid, page,
                        Thread.currentThread().getId());
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return true;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on update record " + rid, e);
    }
  }

  /**
   * Structurally walks the chunk chain of a multi-page record WITHOUT MVCC version validation to detect a broken
   * chain: a continuation pointer to an out-of-range page, a slot that was cleaned, or a marker that is no longer
   * {@link #NEXT_CHUNK}. Used by {@link #check(int, boolean)}, which otherwise validates only the first chunk's
   * on-page size and would never notice a broken chain. Never throws.
   * <p>
   * Returns a short human-readable reason when the chain is broken, and {@code null} when it could not be PROVED
   * broken - which covers both a chain that walks cleanly and one whose walk hit an I/O fault. Every caller reads
   * the two the same way (leave the record alone), and conflating them is deliberate: an I/O fault is not evidence
   * of anything about the record, and since #6258 a confirmed break costs the record its retries and sends the
   * operator to {@code CHECK DATABASE FIX}, which deletes it.
   * <p>
   * The line between the two is drawn by TYPE and not by where a {@code try} sits (#6282): a page that cannot be
   * loaded, or any other {@link IOException}, proves nothing; a {@link PageCorruptionException} - a page that read
   * fine and whose bytes are nonsense - is a break, exactly like the marker and range checks in the walk itself.
   *
   * @param fromNewestCommitted reads the continuation pages straight from the {@link PageManager} instead of through
   *                            the transaction, so the walk sees the newest committed image rather than whatever this
   *                            transaction has pinned. What {@link #confirmBrokenChunkChain} needs (#6258); the
   *                            structural probes that answer for THIS transaction's view pass {@code false}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private String findBrokenChunkChain(final RID rid, final BasePage firstPage, final int firstRecordPositionInPage,
                                      final boolean fromNewestCommitted) {
    return findBrokenChunkChain(rid, firstPage, firstRecordPositionInPage, fromNewestCommitted, new ChunkChainWalk());
  }

  /**
   * @param walk collects the continuation chunks this walk went through, and records whether it reached a conclusion.
   *             The walk needs the set anyway - it is its loop detector - so a caller asking which chunks the record
   *             reaches (#6294) costs nothing beyond keeping it (see {@link ChunkChainWalk}). Reset by this method,
   *             so one instance can be reused across a whole {@link #check} pass.
   */
  private String findBrokenChunkChain(final RID rid, final BasePage firstPage, final int firstRecordPositionInPage,
                                      final boolean fromNewestCommitted, final ChunkChainWalk walk) {
    walk.reset();
    try {
      BasePage chunkPage = firstPage;
      int chunkPositionInPage = firstRecordPositionInPage;
      long[] chunkHeader = firstPage.readNumberAndSize(firstRecordPositionInPage); // [FIRST_CHUNK, headerBytes]
      final int totalPages = getTotalPages();

      // Exact loop detection: a chain can legitimately hold more chunks than the bucket has pages (chunk slots can
      // share pages after chain reuse/fragmentation), so any count-based heuristic risks a false positive - fatal
      // here, because check(fix) DELETES the record it flags. Revisiting a continuation pointer is the only certain
      // loop signal.
      final LongHashSet visitedPointers = walk.chunks;

      for (int chunkId = 0; ; ++chunkId) {
        final long nextChunkPointer = chunkPage.readLong(
                (int) (chunkPositionInPage + chunkHeader[1] + INT_SERIALIZED_SIZE));
        if (nextChunkPointer == 0) {
          // REACHED THE LAST CHUNK CLEANLY. The break location is cleared rather than left pointing at the last
          // SUCCESSFUL hop: both current consumers read it only after a reason came back, but a documented invariant
          // that the code does not keep is a trap for the next one (PR review).
          walk.brokenAtChunk = -1;
          walk.brokenAtPointer = 0;
          return null;
        }

        // FROM HERE UNTIL THE NEXT LAP, ANY FAILURE - RETURNED OR THROWN - HAPPENED FOLLOWING THIS HOP. Recorded up
        // front rather than at each exit because a corrupt slot offset leaves through an exception, and the catch
        // that answers for it is outside the loop and cannot see these (#6282).
        walk.brokenAtChunk = chunkId;
        walk.brokenAtPointer = nextChunkPointer;

        if (!visitedPointers.add(nextChunkPointer))
          return "chain loop detected at chunk " + chunkId;

        final int nextPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int nextPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);
        if (nextPageId >= totalPages)
          return "next chunk pointer out of range at chunk " + chunkId;

        final PageId nextPageIdentity = new PageId(database, file.getFileId(), nextPageId);
        try {
          chunkPage = fromNewestCommitted ?
                  database.getPageManager().getImmutablePage(nextPageIdentity, pageSize, false, true) :
                  database.getTransaction().getPage(nextPageIdentity, pageSize);
        } catch (final IOException e) {
          // A page that cannot be LOADED AT ALL is an I/O fault, not a broken chain, and must not be answered as one
          // - the line #6217 drew for validateChainRead, which this walk had not been held to (code review on #6258).
          // It matters more here than it used to: since #6258 a confirmed break is a PERMANENT verdict that stops the
          // retries and tells the operator to run CHECK DATABASE FIX, which deletes the record. One bad read of a
          // healthy record must not reach that conclusion, and the callers all read null as "cannot prove a break":
          // this walk retries, the tolerant delete keeps its retry semantics, and check() reports nothing to fix.
          LogManager.instance().log(this, Level.FINE,
                  "Unable to load page %s while walking the chunk chain of %s", e, nextPageIdentity, rid);
          walk.incomplete = true;
          walk.brokenAtChunk = -1;
          return null;
        }

        if (chunkPage == null) {
          // Defensive: neither page source answers null while it is allowed to materialise a missing page. A null
          // here would still not be proof of a broken chain, so it must not be reported as one.
          walk.incomplete = true;
          walk.brokenAtChunk = -1;
          return null;
        }

        chunkPositionInPage = getRecordPositionInPage(chunkPage, nextPositionInPage);
        if (chunkPositionInPage == 0)
          return "chunk slot cleaned at chunk " + chunkId;

        chunkHeader = chunkPage.readNumberAndSize(chunkPositionInPage);
        if (chunkHeader[0] != NEXT_CHUNK)
          return "unexpected marker at chunk " + chunkId;
      }
    } catch (final PageCorruptionException e) {
      // THE PAGE WAS READ, ITS CONTENTS ARE NONSENSE: a proven break, and the only shape of IOException that is one.
      // Caught BEFORE the IOException arm below - and the compiler enforces that ordering, which is precisely the
      // point of giving this condition a type of its own (#6282, item 3).
      walk.incomplete = true;
      return "corrupt page content walking the chain: " + e.getMessage();
    } catch (final IOException e) {
      // THE OPPOSITE MEANING, a few lines from the one above: the disk failed, so nothing whatsoever is proved about
      // the record. Answering a break here is what #6258's third review round caught at the page LOAD, and until the
      // corrupt-offset case above got its own type this arm could not be written without swallowing it too.
      LogManager.instance().log(this, Level.FINE, "I/O error while walking the chunk chain of %s", e, rid);
      walk.incomplete = true;
      walk.brokenAtChunk = -1;
      return null;
    } catch (final Exception e) {
      // What is left is the unchecked family the page accessors raise for an offset that leaves the page
      // (IndexOutOfBounds / IllegalArgument / BufferUnderflow, depending on which one caught it first), which is the
      // same "provably bad bytes" signal BucketIterator reads them as. Reported as a break, which is what this has
      // always answered and what {@code check(fix)} deletes the record on. But NOT as a walk that reached a
      // conclusion: the caller that reasons about REACHABILITY is told the walk is incomplete and fails closed - the
      // chunks past this point are not proof of an orphan, and freeing them on this evidence would be unrecoverable
      // (PR review). The two shapes that are NOT bad bytes now leave through their own arms above.
      walk.incomplete = true;
      return "error walking chain: " + e.getMessage();
    }
  }

  /**
   * Structural probe for the tolerant delete path: tells whether the record at {@code rid} is a multi-page record
   * whose chunk chain is structurally broken. Unlike {@link #loadMultiPageRecord} this walk ignores page versions,
   * so a version race on a chain that still walks is not reported as a break. Any failure to probe (including a
   * record that is not multi-page, or is already gone) conservatively returns {@code false}, keeping the caller on
   * the strict retry behaviour.
   * <p>
   * It is the WEAKER of the two questions #6258 asks. {@link #confirmBrokenChunkChain} follows a broken walk of the
   * committed image with a second one - are the chunks the read consumed still, byte for byte, the ones the
   * committed image holds - which is what rules out a chain caught half-published. This has no read to compare
   * against, so it cannot; callers that DO hold the loader's verdict must take that instead, and since #6282 the
   * read path no longer consults this at all.
   * <p>
   * It judges from the NEWEST COMMITTED image of every page and not from the caller's transaction (#6282, item 2).
   * The distinction is invisible under {@code READ_COMMITTED}, which pins nothing, and load-bearing under
   * {@code REPEATABLE_READ}: a transaction that has already walked these pages holds them pinned, so a chain a
   * concurrent commit has since REWRITTEN still presents as broken in the pinned image while walking cleanly in the
   * committed one. What the answer is used for is what makes that unacceptable rather than merely imprecise - every
   * caller escalates a {@code true} to a FORCE delete, and {@code check(fix)} deletes the record it flags, so a
   * false positive here removes a healthy record. The walk it delegates to is the one #6258 already built to confirm
   * a break authoritatively on the read path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  public boolean isChunkChainBroken(final RID rid) {
    try {
      return findBrokenChunkChainInNewestCommitted(rid) != null;
    } catch (final Exception e) {
      // CANNOT PROVE ANYTHING - INCLUDING AN I/O FAULT, WHICH MUST NEVER BECOME A LICENCE TO FORCE-DELETE
      LogManager.instance().log(this, Level.FINE, "Unable to probe the chunk chain of %s", e, rid);
      return false;
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholderContent, final boolean deleteChunks,
      final boolean force) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    // DISJOINT-SLOT MERGE (#5381, #5569): whether this delete is a pure single-slot change depends on the record's
    // marker, which is only known further down once the page has been read - so the poison lives in each non-plain
    // branch below (placeholder pointer or content, chunk chain, corrupted slot, and the error fallback) instead of
    // here. A plain in-place record is deleted by zeroing its slot-table entry (plus the optional content wipe-out),
    // which commutes with writes to every other slot exactly like an insert or an in-page update.
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean slotMergeOn = slotTx != null && slotTx.isSlotMergeEnabled();

    // EDGE-APPEND MERGE: that merge re-derives the page from its committed image plus the tracked appends alone, so
    // a delete left on a page which also received a tracked append would be dropped at rebase time. Freeing a slot
    // is not an append: exclude the page from it, whatever the record shape turns out to be.
    if (slotTx != null)
      slotTx.poisonEdgeAppendPage(fileId, pageId);

    // #5596: the merges this delete's page writes may be replayed by. Raised to COVERAGE_SLOT_MERGE only once the
    // record turns out to be a plain in-place one whose delete really was tracked; every other shape leaves it 0, so
    // the writes stay undeclared and no merge can re-derive the page from them.
    int deleteCoverage = 0;

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
          // The slot holds an 8-byte pointer, and the content record it points to lives on ANOTHER page: not a
          // single-slot change, so the page can never be slot-rebased from this transaction's writes alone.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          // FOUND PLACEHOLDER POINTER: DELETE THE PLACEHOLDER CONTENT FIRST
          final RID placeHolderContentRID = new RID(fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
          try {
            deleteRecordInternal(placeHolderContentRID, true, false, force);
          } catch (RecordNotFoundException e) {
            // PARTIAL RECORD NOT FOUND
          }

        } else if (isChunkHead(recordSize[0])) {
          // The record continues on other pages through a chunk chain: freeing it touches more than this slot.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          // #6196: the head of a placeholder's CONTENT is freed by the same chain walk, but only on behalf of the
          // pointer that references it - a direct delete of it is refused exactly as it is for the negated-size shape
          // further down, which is what keeps a caller from leaving a pointer dangling.
          if (recordSize[0] == FIRST_CHUNK_PLACEHOLDER_CONTENT && !deletePlaceholderContent)
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

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

            // Resolve the next chunk. A broken link (out-of-range pointer, a slot that was cleaned, or a marker that
            // is no longer NEXT_CHUNK) means the chain cannot be walked. Without force the record is left alone, so a
            // half-freed chain is never left behind - as WHICH exception, see the confirmation below. With force
            // (admin repair) the walk stops here instead: the head slot is still freed below, and the chunks past
            // this break are orphaned - a bounded space leak that the orphaned-chunk sweep of check(fix) reclaims
            // (#6294), which is the only thing that does: compaction re-flows LIVE slots, and an orphaned chunk
            // still has one.
            String chainProblem = null;
            try {
              if (chunkPageId >= getTotalPages())
                chainProblem = "next chunk pointer out of range";
              else {
                chunkPage = database.getTransaction()
                        .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
                chunkRecordPositionInPage = getRecordPositionInPage(chunkPage, chunkPositionInPage);
                if (chunkRecordPositionInPage == 0)
                  chainProblem = "chunk slot was cleaned";
                else {
                  recordSize = chunkPage.readNumberAndSize(chunkRecordPositionInPage);
                  if (recordSize[0] != NEXT_CHUNK)
                    chainProblem = "chunk marker is not NEXT_CHUNK";
                }
              }
            } catch (final PageCorruptionException e) {
              // The page read fine and its bytes are nonsense: a STRUCTURAL problem exactly like the three above,
              // and one no retry can resolve. It used to leave here as a plain IOException, which the arm below
              // rethrew to a caller with no way to tell it from a disk that had just failed (#6282, item 3).
              chainProblem = "corrupt chunk slot: " + e.getMessage();
            } catch (final IOException e) {
              if (!force)
                throw e;
              chainProblem = "error reading chunk: " + e.getMessage();
            }

            if (chainProblem != null) {
              if (!force) {
                // CORRUPTION IS NOT CONTENTION (#6258), and until #6282 this path said otherwise: it raised the
                // #4932 retry signal for a chain it had just positively identified as structurally broken, costing
                // the caller a full round of transactions before it gave up and forcing five call sites to walk the
                // chain a SECOND time just to find out which of the two they were holding.
                final String confirmed = confirmBrokenChunkChainOnDelete(rid, chunkId, nextChunkPointer);
                if (confirmed != null)
                  throw new BrokenChunkChainException(
                          "Multi-page record " + rid + " has a broken chunk chain at chunk " + chunkId + " ("
                                  + chainProblem + (confirmed.equals(chainProblem) ?
                                  "" :
                                  "; the committed image reports: " + confirmed)
                                  + "): the record is corrupted and cannot be deleted. Run CHECK DATABASE FIX to "
                                  + "repair it, or enable " + GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN.getKey()
                                  + " to force the delete through");

                // Chunk was modified/removed by a concurrent operation - signal retry (#4932)
                throw new ConcurrentModificationException(
                        "Multi-page record " + rid + " chunk " + chunkId + " was modified concurrently. Please retry");
              }

              LogManager.instance().log(this, Level.WARNING,
                      "Force-deleting multi-page record %s with a broken chunk chain at chunk %d (%s); orphaned chunks (if "
                              + "any) are reclaimed by CHECK DATABASE FIX.", rid, chunkId, chainProblem);
              break;
            }

            try {
              deleteRecordInternal(new RID(fileId, nextChunkPointer), false, true, force);
            } catch (RecordNotFoundException e) {
              // PARTIAL RECORD NOT FOUND
            }
          }

        } else if (recordSize[0] == NEXT_CHUNK) {
          // A continuation chunk of a record whose head lives elsewhere: part of a multi-page structure.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (!deleteChunks)
            // CANNOT DELETE A CHUNK DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          // Placeholder CONTENT: it is reachable only through a pointer on another page, so rebasing this page in
          // isolation would leave that pointer dangling.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (!deletePlaceholderContent)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
        } else if (recordSize[0] > 0) {
          // PLAIN IN-PLACE RECORD: the delete is a single-slot change (zero the slot-table entry, optionally wipe the
          // content), so it commutes with concurrent writes to the other slots of the page and can be replayed on the
          // newer committed version at commit time (#5569). The pre-image must be captured BEFORE the wipe-out below,
          // and lets the replay tell a false page conflict from a concurrent write to THIS record.
          if (slotMergeOn) {
            if (page.readByte((int) (recordPositionInPage + recordSize[1])) == EdgeSegment.RECORD_TYPE)
              // An edge-list segment is owned by the commutative edge-append merge, not by this one: keep the two
              // mechanisms from ever rebasing the same page (mirrors isSlotMergeCandidate on the update path).
              slotTx.poisonSlotRebasePage(fileId, pageId);
            else if (!slotTx.isSlotRebasePagePoisoned(fileId, pageId)) {
              final byte[] baseBody = new byte[(int) recordSize[0]];
              page.readByteArray((int) (recordPositionInPage + recordSize[1]), baseBody, 0, baseBody.length);
              slotTx.trackRebasableDelete(fileId, pageId, positionInPage, baseBody);
              // #5596: from here on the two writes this delete makes - the content wipe-out and the slot-pointer
              // zeroing below - are exactly what rebaseRecordDeleteOnPage re-applies, so declare them covered.
              deleteCoverage = MutablePage.COVERAGE_SLOT_MERGE;
            }
          }

          if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
            // WIPE OUT RECORD CONTENT
            final int previousCoverage = page.beginCoveredWrite(deleteCoverage);
            try {
              page.writeZeros(recordPositionInPage + 1, (int) (recordSize[0] + recordSize[1] - 1));
            } catch (Exception e) {
              // IGNORE IT
              LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
            } finally {
              page.endCoveredWrite(previousCoverage);
            }
          }
        } else if (slotMergeOn)
          // AN UNKNOWN NEGATIVE MARKER: NOT A PLAIN SINGLE-SLOT RECORD, KEEP THE PAGE OUT OF THE MERGE
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // POINTER = 0 MEANS DELETED. deleteCoverage is COVERAGE_SLOT_MERGE only for the tracked plain-record delete
        // above; every other shape leaves it 0, so the page cannot be slot-rebased (#5596).
        final int previousCoverage = page.beginCoveredWrite(deleteCoverage);
        try {
          page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
        } finally {
          page.endCoveredWrite(previousCoverage);
        }
        freedWithoutClaiming(page);

        // Track deleted RID to prevent reuse within the same transaction
        database.getTransaction().addDeletedRecord(rid);

        // #6339: the delete no longer tells the free-space statistics anything, because everything it used to tell
        // them was wrong. It measured the page AFTER zeroing the slot - so the tail the delete had just released was
        // already in the number - and then subtracted the record's footprint from it, landing back on exactly the
        // free space the page had BEFORE the delete; mid-page it subtracted a footprint from a tail that had not
        // moved at all, and could drop the page's entry outright. It also skipped every negative marker, i.e. every
        // shape that is not a plain record, and tested that marker on `recordSize` AFTER the chunk-chain walk above
        // had reassigned it to the last chunk of the chain. What the page really has to offer is measured once, on
        // the packed page, by the compression the commit runs on it (compressPageInternal).

      } else {
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD. There is no readable pre-image to check the replay
        // against, so the page cannot take part in the slot merge.
        if (slotMergeOn)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
        freedWithoutClaiming(page);
      }

      LogManager.instance()
              .log(this, Level.FINE, "Deleted record %s (%s threadId=%d)", null, rid, page, Thread.currentThread().getId());

    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final BrokenChunkChainException e) {
      // #6282: the corruption verdict thrown above, and it needs this arm for exactly the reason the #4932 one below
      // does - the generic catch would swallow it, zero the slot pointer and report success, orphaning every chunk
      // past the break and telling the caller a corrupted record had been deleted cleanly. Rethrow so the operator
      // hears it, and so the tolerant path can decide whether to force the delete through.
      throw e;
    } catch (final ConcurrentModificationException e) {
      // #4932: this is the retry signal deliberately thrown above when a multi-page chunk chain was modified
      // concurrently. The generic catch below used to swallow it, zero the slot pointer and return success:
      // the retry never happened, the remaining NEXT_CHUNK records were orphaned (permanent space leak) and
      // the caller believed the delete succeeded. Rethrow so the retry machinery actually retries.
      throw e;
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on deletion of record " + rid, e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on deleting record %s content", e, rid);

      if (page != null) {
        // The delete failed half-way, so whatever was tracked for this page no longer describes it: never let the
        // slot merge re-derive the page from it.
        if (slotMergeOn)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
        freedWithoutClaiming(page);
      }
    }
  }

  /**
   * Defragments the page in place, closing the holes an update or a delete left behind.
   * <p>
   * #5596: with {@code forceWipeOut=false} every byte written here is declared covered by ALL the commit-time page
   * merges. That is sound - and necessary to keep them effective - because the merge re-runs exactly this call on the
   * page it re-derived, so the compression is reproduced rather than lost: it is a layout-only transformation of
   * whatever records the page ends up holding, not a change of this transaction's own. A FORCED wipe-out (the
   * database checker) is not re-run by the merge path, so it declares nothing and disqualifies the page.
   * <p>
   * #5608: that re-run is the load-bearing half of the declaration, so it lives INSIDE the two methods that produce a
   * rebased page ({@code TransactionContext.rebaseEdgeAppends} and {@code rebaseSlots}) rather than in the commit loop
   * that calls them - where reordering or dropping it would have invalidated the declaration with no test failing.
   * {@code Issue5608RebasedPageCompressionTest} pins the invariant from the outside.
   */
  public void compressPage(final MutablePage page, final boolean forceWipeOut) throws IOException {
    final int previousCoverage = page.beginCoveredWrite(forceWipeOut ? 0 : MutablePage.COVERAGE_ALL_MERGES);
    try {
      compressPageInternal(page, forceWipeOut);
    } finally {
      page.endCoveredWrite(previousCoverage);
    }
  }

  private void compressPageInternal(final MutablePage page, final boolean forceWipeOut) throws IOException {
    final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

    final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage, page);

    // #6396: the one moment both descriptions of this page's free tail exist at once - what the writes SAID it would
    // be, and what the page HAS. See verifyFreeSpaceClaim; the derivation has to happen BEFORE the defrag below moves
    // the records.
    if (CHECK_FREE_SPACE_CLAIMS)
      verifyFreeSpaceClaim(page, orderedRecordContentInPage);

    if (orderedRecordContentInPage.isEmpty()) {
      if (recordCountInPage > 0) {
        // RESET RECORD COUNTER TO 0
        page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 0);
        LogManager.instance().log(this, Level.FINE, "Update record count from %d to 0 in page %s", recordCountInPage, page.pageId);
        wipeOutFreeSpace(page, (short) 0);
      }
      accountCompressedPage(page, contentHeaderSize);
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

    // #6339: the page's occupancy is settled now, and this is the one place that knows it exactly - the records were
    // just packed from contentHeaderSize upwards, so their footprints sum to where the free tail begins. Everything
    // the free-space statistics are told anywhere else is a delta an individual write knows about its own bytes; this
    // is the whole page, measured, and it is what closes the gap the deltas cannot: a hole in the middle of a page is
    // NOT free tail while it is a hole - the allocator hands out the tail and nothing else - and it stops being one
    // right here.
    int contentEndInPage = contentHeaderSize;
    for (final int[] record : orderedRecordContentInPage)
      contentEndInPage += record[1];
    accountCompressedPage(page, contentEndInPage);
  }

  /**
   * The caller is about to give bytes back on this page - free a slot, overwrite a record with a shorter one - and,
   * deliberately, will not tell the free-space statistics: the compression the commit runs measures the packed page
   * anyway, and accounting at the free itself was measured to be worth less than the slot-table scan per freed slot
   * it costs (#6339). So whatever a previous write claimed about the page's free tail stops being that tail and stays
   * a floor under it (#6396): the check that follows can no longer demand equality, but it still catches the writer
   * that claims MORE free space than the page has.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static void freedWithoutClaiming(final MutablePage page) {
    if (CHECK_FREE_SPACE_CLAIMS)
      page.relaxFreeSpaceClaim();
  }

  /**
   * The free TAIL of a page: everything past the last record it holds, measured against
   * {@link BasePage#getMaxContentSize()} - the usable content region, never {@code getAvailableContentSize()}, which
   * still counts the page header (#4958, #5067). The quantity every write path reports to the free-space statistics,
   * derived in ONE place so the scan that reads a page it did not write ({@link #gatherPageStatistics}) and the check
   * that holds the writers to it ({@link #verifyFreeSpaceClaim}) cannot come to describe it differently - which is
   * the very failure #6396 exists to make loud.
   *
   * @param orderedRecordsInPage the page's live records in position order, as {@link #getOrderedRecordsInPage}
   *                             returns them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private int freeTailInPage(final BasePage page, final List<int[]> orderedRecordsInPage) {
    if (orderedRecordsInPage.isEmpty())
      return page.getMaxContentSize() - contentHeaderSize;

    final int[] lastRecord = orderedRecordsInPage.get(orderedRecordsInPage.size() - 1);
    return page.getMaxContentSize() - (lastRecord[0] + lastRecord[1]);
  }

  /**
   * Holds what a transaction's writes SAID this page's free tail would be against what the page, read here, actually
   * has (#6396).
   * <p>
   * Everything the free-space statistics are told by a write is a delta that write computes about its own bytes,
   * and nothing observable after a commit depends on it being right: every page a transaction touches is compressed
   * at commit, {@link #compressPageInternal} MEASURES its free tail there and {@link #accountCompressedPage}
   * overwrites whatever the deltas had accumulated. So a writer could be - and, until #6154, #6339 and #6358 were
   * each opened for one, repeatedly was - thousands of bytes wrong without a single red test. Two independent
   * descriptions of one quantity, and no mechanism keeping them honest.
   * <p>
   * This is that mechanism, and it needs no fixture: every commit in every test becomes a check of the write-path
   * arithmetic. The comparison is an EQUALITY, against the tail the page has BEFORE the defrag - which is exactly
   * what a write reports, holes and all - rather than against the packed tail {@code accountCompressedPage} goes on
   * to record, because that one is larger by every hole the compression is about to close and a claim short by the
   * same amount would slip through it.
   * <p>
   * A write that GIVES BYTES BACK and deliberately does not report them - a delete (#6339), an in-place update that
   * shrank - demotes the claim to a lower bound ({@link #freedWithoutClaiming}) instead of dropping it, so the
   * equality relaxes to {@code <=} on that page and the over-reporting direction stays checked everywhere. A page no
   * write has spoken about carries {@link MutablePage#FREE_SPACE_CLAIM_UNKNOWN} and is skipped. If this fires on a
   * NEW write path, the answer is a corrected delta or that one call, never a widening of the comparison.
   *
   * @param orderedRecordContentInPage the page's live records in position order, as the compression read them and
   *                                   before it moved any of them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void verifyFreeSpaceClaim(final MutablePage page, final List<int[]> orderedRecordContentInPage) {
    final int claimed = page.getFreeSpaceClaim();
    if (claimed == MutablePage.FREE_SPACE_CLAIM_UNKNOWN)
      return;

    // The tail the page has right now, through the same derivation gatherPageStatistics measures an unwritten page
    // with: comparing a claim against a second opinion of the quantity would be the defect this check is for.
    final int freeTailInPage = freeTailInPage(page, orderedRecordContentInPage);

    final boolean exact = page.isFreeSpaceClaimExact();
    assert exact ? claimed == freeTailInPage : claimed <= freeTailInPage :
        "page " + page.getPageId() + " of bucket '" + componentName + "' was reported to the free-space statistics with "
            + claimed + (exact ? " free bytes" : " free bytes or more") + ", but holds " + freeTailInPage + " ("
            + orderedRecordContentInPage.size() + " records). A write described bytes it did not write: correct its "
            + "delta, or call freedWithoutClaiming() if it gives bytes back without reporting them";
  }

  /**
   * Records, in the free-space statistics, the free tail a page has once {@link #compressPageInternal} has packed it
   * (#6339). The delta convention of {@link #updatePageStatistics} carries an absolute value as
   * {@code (theSpaceThereIs, 0)}, which is what this is: a measurement, not a change - and a page packed to its
   * maximum content size lands on 0 and has its entry dropped, exactly as a page a spill has sealed does.
   * <p>
   * Costs nothing to derive: the sum comes from the ordered record list the compression already built.
   * <p>
   * What it trades, stated so the next reader does not take it for an oversight: a record deleted and another
   * inserted inside the SAME still-open transaction do not see the freed space as a hint, because the page is packed
   * at commit and not before. The allocator is never given a wrong answer by this - {@code findAvailableSpace}
   * re-reads every candidate page - only a placement it could have made and did not, and the freed page is offered
   * from the commit onwards. It was measured before being accepted: an accounting at the free itself, which is the
   * only thing that could close that window, moved 4 pages out of 239 on a transaction that deletes two thirds of
   * 4000 records and re-inserts as many, and cost a slot-table scan per freed slot to do it.
   *
   * @param contentEndInPage first free byte of the packed page, i.e. where its free tail starts.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void accountCompressedPage(final MutablePage page, final int contentEndInPage) {
    updatePageStatistics(page, page.getMaxContentSize() - contentEndInPage, 0);
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
        final int[] lastHole = holes.isEmpty() ? null : holes.get(holes.size() - 1);
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

  /**
   * READ-ONLY walk of a page's slot table: returns the {@code {offset, footprint}} of every record the page holds,
   * ordered by offset, and writes nothing at all.
   * <p>
   * #6441: this overload exists so that a caller with nothing but a {@link BasePage} in hand - a statistics scan, a
   * check - cannot free a slot as a side effect of reading. The repair belongs to
   * {@link #getOrderedRecordsInPage(BasePage, short, MutablePage)} below, whose signature demands the
   * {@link MutablePage} the caller already owns.
   */
  private List<int[]> getOrderedRecordsInPage(final BasePage page, final short recordCountInPage) {
    return getOrderedRecordsInPage(page, recordCountInPage, null);
  }

  /**
   * The same walk, optionally FREEING the slots it cannot make sense of: a slot deleted by a pre-24.1.1 engine (which
   * left the record table entry pointing at a zero-length record instead of zeroing it), and a slot whose declared
   * size cannot fit the page. Both are excluded from the returned list either way; {@code repairOn} decides whether
   * the record table entry that produced them is also zeroed.
   * <p>
   * #6441: the two branches used to be guarded on a {@code readOnly} flag in OPPOSITE directions, so the invalid-size
   * slot was freed by the statistics scan - which reached {@code database.getTransaction().getPageToModify()} from
   * inside an ordinary allocation and enlisted an unrelated page into whatever transaction happened to be inserting
   * a record - and walked past by the compression, the one caller that already holds the page for writing. A
   * {@code MutablePage} parameter states the same thing the flag was meant to state, except that the compiler
   * enforces it and there is no silent {@code getPageToModify} upgrade left to reintroduce it.
   * <p>
   * Freeing the slot here needs no {@code poisonSlotRebasePage} (unlike the corrupted-record arm of
   * {@link #deleteRecordInternal}): the only caller is {@link #compressPageInternal}, whose writes are declared
   * covered by ALL the commit-time merges precisely because the merge re-runs the compression on the page it
   * re-derived - so the repair is reproduced there rather than lost.
   *
   * @param repairOn the page to free unreadable slots on. Must be the very page being walked.
   */
  private List<int[]> getOrderedRecordsInPage(final BasePage page, final short recordCountInPage, final MutablePage repairOn) {
    assert repairOn == null || repairOn == page : "the repair must land on the page being walked";

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
          if (repairOn != null)
            // SET 0 IN THE RECORD TABLE
            freeCorruptedSlot(repairOn, positionInPage);
          continue;
        }

        if (recordSize[0] == RECORD_PLACEHOLDER_POINTER)
          size = LONG_SERIALIZED_SIZE + (int) recordSize[1];
        else if (isChunkHead(recordSize[0]) || recordSize[0] == NEXT_CHUNK) {
          final int chunkSize = page.readInt(recordPositionInPage + (int) recordSize[1]);
          size = chunkFootprint((int) recordSize[1], chunkSize);
        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT)
          // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
          size = (int) (-1 * recordSize[0]) + (int) recordSize[1];
        else
          size = (int) recordSize[0] + (int) recordSize[1];

        if (size < 0 || size > getPageSize() - contentHeaderSize) {
          // INVALID SIZE. Say what was actually done with it: a read-only walk skips it and leaves it for the next
          // compression, and only the compression deletes it (#6441). The message used to claim the deletion on the
          // walk that performed none, so an operator reading it was told a record had gone when nothing had.
          LogManager.instance().log(this, Level.SEVERE,
                  "Invalid record size " + size + " for record #" + fileId + ":"
                          + recordPosition(page.pageId.getPageNumber(), maxRecordsInPage, positionInPage)
                          + (repairOn != null ?
                          ": deleting record" :
                          ": skipping record (run `CHECK DATABASE COMPRESS` to free the slot)"));

          if (repairOn != null)
            freeCorruptedSlot(repairOn, positionInPage);
          continue;
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
                "Error on loading record #" + fileId + ":" + recordPosition(page.pageId.getPageNumber(), maxRecordsInPage, positionInPage));
        continue;
      }

      orderedRecordContentInPage.add(new int[] { recordPositionInPage, size });
    }

    orderedRecordContentInPage.sort(Comparator.comparingLong(a -> a[0]));

    return orderedRecordContentInPage;
  }

  /**
   * Zeroes a record-table entry the compression could not make sense of, and invalidates the cached record count
   * along with it.
   * <p>
   * #6441: this write, like every corrupt-record deletion in {@link #deleteRecordInternal} (see {@link #check}'s own
   * comment on the same subject), registers no bucket delta - it does not go through the transaction's
   * insert/delete bookkeeping, because it is a physical-layer repair rather than a semantic one. A record that was
   * live-counted (inserted and counted this session, not a shape inherited from a pre-24.1.1 database) would
   * otherwise leave {@code cachedRecordCount} one too high forever: nothing else would ever notice the slot is
   * gone. Invalidating costs nothing on the common path - this only runs at all when a slot could not be read - and
   * makes the next {@link #count()} fall through to its own authoritative scan instead of serving a stale number.
   */
  private void freeCorruptedSlot(final MutablePage repairOn, final int positionInPage) {
    repairOn.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
    cachedRecordCount.set(-1);
  }

  /**
   * Loads a multi-page record, validating that what it assembled is one committed state of the record and not a mix
   * of two.
   * <p>
   * Under READ_COMMITTED isolation (the default) the transaction caches no page, so every chunk of the chain is
   * loaded independently and a concurrent commit landing between two of those loads would otherwise be assembled into
   * a record that never existed - typically a truncated one, surfacing as a {@code BufferUnderflowException} on
   * deserialization. A read that finds such a mix is retried (up to {@link GlobalConfiguration#TX_RETRIES} times), so
   * read-only queries absorb ordinary contention instead of failing with a
   * {@link ConcurrentModificationException}.
   * <p>
   * <b>What is validated is the RECORD, not the pages it lives on (#6217).</b> This walk used to re-check the VERSION
   * of every page the chain touched, and a page version moves when ANY record on it moves - continuation chunks of
   * different records share pages because the allocator packs them there on purpose - so a reader was failed by
   * writes that had not touched a single byte of its own record, and after the retry budget it raised a
   * {@code ConcurrentModificationException} on an untouched record. It is the read-path twin of the false conflict
   * the disjoint-slot merge removed from the write path in #5381/#6129/#6175.
   * <p>
   * The version is still the FAST path - a page that has not moved cannot hold a chunk that has - but a page that HAS
   * moved is now asked the precise question instead: is the chunk this record owns on it still, byte for byte, the
   * one this read consumed? Marker, declared size, next-chunk pointer and content are compared against the trace kept
   * during the walk ({@link #CHAIN_TRACE_STRIDE}), which covers the chain's shape as well as its bytes: a chunk that
   * moved to another slot, a chain that gained or lost one, a head chunk that collapsed back into a plain record
   * (#6178) all change one of the four. When every chunk answers yes, the assembled record is exactly what a fresh
   * read of the newest committed state would produce, so the read stands.
   * <p>
   * A byte comparison rather than the 64-bit fold {@code offPageContentFingerprint} uses on the commit side: it costs
   * the same walk, is paid only on a page that actually moved, and unlike a hash it cannot be wrong. The commit side
   * has to compare two points in TIME and can only carry a number across them; a read holds both images at once.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private Binary loadMultiPageRecord(final RID originalRID, BasePage firstPage, int recordPositionInPage,
                                     long[] recordSize) throws IOException {
    final int maxRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
    final PageId firstPageId = firstPage.pageId;
    final int firstChunkSlot = (int) (originalRID.getPosition() % maxRecordsInPage);

    for (int retry = 0; retry <= maxRetries; retry++) {
      // Trace of the chunks this attempt consumed, in chain order: where each one was, at which version of its page,
      // and how long it was. Sized for the chains that exist in practice and grown only by a record that has more.
      long[] chainTrace = new long[CHAIN_TRACE_STRIDE * 8];
      int chunks = 0;
      // The continuation pointer the LAST traced chunk carried, so a chain the walk could not finish can be validated
      // too: for a chain walked to its end this is 0, which is what the last chunk really holds.
      long lastNextChunkPointer = 0L;

      // Set ONLY by the branches that could not PARSE the chain, never by the read-vs-write validation below: the two
      // have opposite answers and used to share one flag (#6258).
      String brokenChainReason = null;
      Exception brokenChainCause = null;

      // Which kind of head this attempt started from - a record's own, or a placeholder's CONTENT (#6196). Read here
      // rather than assumed, because a retry re-reads the slot and could find the other one.
      final long headMarker = recordSize[0];

      // Exact cycle detection, on the same terms as offPageContentFingerprint's walk: revisiting a continuation
      // pointer is the only certain loop signal, and a chain that never revisits one is finite by construction. The
      // set is allocated only once a chain turns out to be long, so the records that exist in practice - a handful of
      // chunks - pay nothing for it. The self-reference check further down stays: it is free, and it names the loop
      // for the chain that doubles back on its very first hop, long before this threshold (code review on #6258).
      LongHashSet visitedChunks = null;

      boolean chainInconsistent = false;
      final Binary record = new Binary();
      try {
        BasePage page = firstPage;
        int currentSlot = firstChunkSlot;
        int currentRecordPositionInPage = recordPositionInPage;
        long[] currentRecordSize = recordSize;

        while (true) {
          final int chunkSize = page.readInt((int) (currentRecordPositionInPage + currentRecordSize[1]));
          final long nextChunkPointer = page.readLong(
                  (int) (currentRecordPositionInPage + currentRecordSize[1] + INT_SERIALIZED_SIZE));
          final Binary chunk = page.getImmutableView(
                  (int) (currentRecordPositionInPage + currentRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE),
                  chunkSize);
          record.append(chunk);

          if (chunks * CHAIN_TRACE_STRIDE == chainTrace.length)
            chainTrace = Arrays.copyOf(chainTrace, chainTrace.length * 2);
          final int trace = chunks * CHAIN_TRACE_STRIDE;
          chainTrace[trace + CHAIN_TRACE_PAGE_NUMBER] = page.pageId.getPageNumber();
          chainTrace[trace + CHAIN_TRACE_PAGE_VERSION] = page.getVersion();
          chainTrace[trace + CHAIN_TRACE_SLOT] = currentSlot;
          chainTrace[trace + CHAIN_TRACE_CHUNK_SIZE] = chunkSize;
          ++chunks;
          lastNextChunkPointer = nextChunkPointer;

          if (nextChunkPointer == 0)
            break;

          final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
          final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

          // Every reason below names the chunk the failing HOP LEFT, which is the numbering findBrokenChunkChain
          // already uses - the two walks report the same break in the same words.
          if (chunkPageId >= getTotalPages()) {
            brokenChainReason = "next chunk pointer out of range at chunk " + (chunks - 1);
            break;
          }

          final BasePage nextPage = database.getTransaction()
                  .getPage(new PageId(database, file.getFileId(), chunkPageId), pageSize);

          final int nextRecordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
          if (nextRecordPositionInPage == 0) {
            brokenChainReason = "chunk slot cleaned at chunk " + (chunks - 1);
            break;
          }

          if (nextPage.equals(page) && currentRecordPositionInPage == nextRecordPositionInPage) {
            brokenChainReason = "chain loop detected at chunk " + (chunks - 1) + " (" + chunkPageId + "/"
                    + chunkPositionInPage + ")";
            break;
          }

          if (chunks >= CHUNKS_BEFORE_LOOP_DETECTION) {
            if (visitedChunks == null)
              visitedChunks = new LongHashSet();
            if (!visitedChunks.add(nextChunkPointer)) {
              brokenChainReason = "chain loop detected at chunk " + (chunks - 1) + " (" + chunkPageId + "/"
                      + chunkPositionInPage + " visited twice)";
              break;
            }
          }

          page = nextPage;
          currentSlot = chunkPositionInPage;
          currentRecordPositionInPage = nextRecordPositionInPage;

          currentRecordSize = page.readNumberAndSize(currentRecordPositionInPage);

          if (currentRecordSize[0] != NEXT_CHUNK) {
            brokenChainReason = "unexpected marker at chunk " + (chunks - 1);
            break;
          }
        }
      } catch (final Exception e) {
        brokenChainReason = "error walking the chain: " + e.getMessage();
        brokenChainCause = e;
      }

      if (brokenChainReason != null) {
        // A CHAIN THAT DOES NOT PARSE IS NOT CONTENTION (#6258). Retrying can only change the answer if the chain the
        // walk followed was not the committed one, so ask exactly that before spending the budget: prove the break
        // first against the newest committed image and then against this read's own chunks. When both agree, the
        // record is corrupted and every further attempt would walk into the same dead end and report a concurrency
        // problem that does not exist.
        final String confirmed = confirmBrokenChunkChain(originalRID, chainTrace, chunks, record, lastNextChunkPointer,
                headMarker);
        if (confirmed != null)
          // Both reasons when they differ, and they can: this walk carries a loop detector the confirmation walk
          // answers as the wrong marker it finds where the chain doubles back. The first is what the READ hit, the
          // second what the committed image says - one diagnosis is not more true than the other, and an operator
          // holding a corrupted record wants the pair.
          throw new BrokenChunkChainException(
                  "Multi-page record " + originalRID + " has a broken chunk chain (" + brokenChainReason
                          + (confirmed.equals(brokenChainReason) ? "" : "; the committed image reports: " + confirmed)
                          + "): the record is corrupted and cannot be read. Run CHECK DATABASE FIX to repair it",
                  brokenChainCause);

        chainInconsistent = true;
      } else {
        // OUTSIDE the catch above, deliberately and as the page-version loop this replaced already was: everything
        // the validation reads OUT OF A PAGE is answered by isChunkStillTheOneRead, which never throws and reads a
        // chunk it cannot make sense of as a chunk that changed. What is left to come out of here is the failure to
        // LOAD a page at all, which is an I/O error and not a conflict - absorbing it would spend the retry budget
        // on a broken disk and then report it as "the record was modified during read".
        final int verdict = validateChainRead(chainTrace, chunks, record, lastNextChunkPointer, headMarker);
        if (verdict == CHAIN_READ_REVALIDATED)
          database.getPageManager().incrementChunkChainReadRevalidations();
        chainInconsistent = verdict == CHAIN_READ_CHANGED;
      }

      if (!chainInconsistent) {
        record.position(0);
        return record;
      }

      database.getPageManager().incrementChunkChainReadRetries();

      // Retry by re-fetching the first page with fresh data
      if (retry < maxRetries) {
        final BasePage refreshedFirstPage = database.getPageManager().getImmutablePage(firstPageId, pageSize, false, true);
        if (refreshedFirstPage == null)
          throw new ConcurrentModificationException(
                  "First page of multi-page record " + originalRID + " was removed during read");

        if (!aRetryWouldReadSomethingElse(refreshedFirstPage, chainTrace, chunks))
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " was modified during read and cannot be re-read in this "
                          + "transaction: its chunks are pinned by the transaction's own snapshot, so no retry can "
                          + "assemble a different one. Please retry the operation in a new transaction");

        LogManager.instance().log(this, Level.FINE,
                "Multi-page record %s read inconsistent (attempt %d/%d), retrying...", originalRID,
                retry + 1, maxRetries);
        firstPage = refreshedFirstPage;
        recordPositionInPage = getRecordPositionInPage(firstPage, firstChunkSlot);
        if (recordPositionInPage == 0)
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " was deleted during read");
        recordSize = firstPage.readNumberAndSize(recordPositionInPage);
      } else
        throw new ConcurrentModificationException(
                "Multi-page record " + originalRID + " was modified during read after " + maxRetries
                        + " retries. Please retry the operation");
    }

    throw new DatabaseOperationException("Failed to load multi-page record " + originalRID);
  }

  /**
   * Decides whether the chain {@link #loadMultiPageRecord} just walked is still the one it read (#6217).
   * <p>
   * Every chunk is checked against the NEWEST committed image of its page: unchanged page version means unchanged
   * chunk and costs a cache lookup, a moved page is compared byte for byte against what the read consumed. A page
   * that cannot be read at all is left alone, exactly as the version check that preceded this did - there is nothing
   * to compare against, and failing a read on it would be inventing a conflict.
   *
   * @param lastNextChunkPointer the continuation pointer the last traced chunk carried: 0 for a chain walked to its
   *                             end, and the pointer that could not be followed for a walk the chain broke (#6258) -
   *                             which is what lets a broken chain be validated by the same comparison as a whole one.
   * @param headMarker           the marker the head chunk carried when the walk started, {@link #FIRST_CHUNK} or
   *                             {@link #FIRST_CHUNK_PLACEHOLDER_CONTENT} (#6196). Carried rather than assumed because
   *                             it is part of what the comparison checks: a head whose marker changed under the read
   *                             is a slot that stopped being what it was, exactly like one whose size or pointer did.
   *
   * @return {@link #CHAIN_READ_UNCHANGED}, {@link #CHAIN_READ_REVALIDATED} or {@link #CHAIN_READ_CHANGED}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private int validateChainRead(final long[] chainTrace, final int chunks, final Binary record,
                                final long lastNextChunkPointer, final long headMarker) throws IOException {
    int verdict = CHAIN_READ_UNCHANGED;
    int contentOffset = 0;

    for (int chunk = 0; chunk < chunks; ++chunk) {
      final int trace = chunk * CHAIN_TRACE_STRIDE;
      final int chunkSize = (int) chainTrace[trace + CHAIN_TRACE_CHUNK_SIZE];

      final BasePage currentPage = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), (int) chainTrace[trace + CHAIN_TRACE_PAGE_NUMBER]),
                      pageSize, false, true);

      if (currentPage != null && currentPage.getVersion() != chainTrace[trace + CHAIN_TRACE_PAGE_VERSION]) {
        // The pointer this chunk carries is the NEXT chunk's own (page, slot), so the chain's shape is checked with
        // the trace and nothing else: a chain that gained or lost a chunk, or whose next chunk was relocated, fails
        // here even when every byte of content is the same. The last chunk is the one exception - there is no next
        // trace entry to derive it from - so the walk hands its pointer over.
        final long nextChunkPointer = chunk + 1 < chunks ?
                chainTrace[(chunk + 1) * CHAIN_TRACE_STRIDE + CHAIN_TRACE_PAGE_NUMBER] * (long) maxRecordsInPage
                        + chainTrace[(chunk + 1) * CHAIN_TRACE_STRIDE + CHAIN_TRACE_SLOT] :
                lastNextChunkPointer;

        if (!isChunkStillTheOneRead(currentPage, (int) chainTrace[trace + CHAIN_TRACE_SLOT],
                chunk == 0 ? headMarker : NEXT_CHUNK, chunkSize, nextChunkPointer, record, contentOffset))
          return CHAIN_READ_CHANGED;

        verdict = CHAIN_READ_REVALIDATED;
      }

      contentOffset += chunkSize;
    }

    return verdict;
  }

  /**
   * Whether another attempt at {@link #loadMultiPageRecord} could possibly read anything other than what this one
   * just did (#6258).
   * <p>
   * The retry exists for {@code READ_COMMITTED}, where the transaction caches no page: every attempt re-reads the
   * whole chain from the {@link PageManager}, so a read torn by a commit landing mid-walk genuinely reassembles
   * cleanly on the next pass. Under {@code REPEATABLE_READ} it is a different machine. The retry re-fetches the head
   * page straight from the page manager, deliberately bypassing the transaction, but the walk takes its continuation
   * pages from {@link TransactionContext#getPage}, which serves them from the snapshot this transaction has already
   * pinned. So every attempt pairs a fresh head with the very same tails, reproduces the very same mix, and is
   * rejected for the very same reason - the whole retry budget spent on a verdict that was settled before the first
   * retry started, and spent on the slowest read path there is, under contention, on the largest records.
   * <p>
   * The question is answered from the trace rather than from the isolation level, because the isolation level is not
   * actually what decides it: a retry is worth making when the head page has MOVED (the next walk starts from
   * different bytes, and may well follow the chain somewhere else entirely), or when any continuation page is one
   * this transaction has not pinned (it will be re-read, and can differ). When neither holds, the next walk reads
   * byte for byte what this one read, and the read fails now instead of three chain walks from now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private boolean aRetryWouldReadSomethingElse(final BasePage refreshedFirstPage, final long[] chainTrace,
                                               final int chunks) {
    if (chunks == 0)
      // Nothing was read: there is no evidence a retry is futile.
      return true;

    if (refreshedFirstPage.getVersion() != chainTrace[/* chunk 0 */ CHAIN_TRACE_PAGE_VERSION])
      // The head chunk this walk started from is no longer the committed one.
      return true;

    final TransactionContext transaction = database.getTransactionIfExists();
    if (transaction == null)
      return true;

    for (int chunk = 1; chunk < chunks; ++chunk) {
      final PageId pageId = new PageId(database, file.getFileId(),
              (int) chainTrace[chunk * CHAIN_TRACE_STRIDE + CHAIN_TRACE_PAGE_NUMBER]);
      if (!transaction.hasPageForRecord(pageId))
        // Not pinned by this transaction: the next walk reloads it, and it can come back different.
        return true;
    }

    return false;
  }

  /**
   * Confirms that a chunk chain {@link #loadMultiPageRecord} could not parse is genuinely broken rather than a
   * record that moved under the read (#6258), and returns the reason it is broken, or {@code null} when the break
   * cannot be told apart from contention - in which case the caller retries exactly as it always did.
   * <p>
   * Two questions, in this order, because the second is the guard for the first:
   * <ol>
   * <li>Does the chain still fail to walk on the NEWEST committed image of its pages? A commit publishes its pages
   * one at a time and readers take no lock, so a chain caught mid-publication can look broken for a few microseconds
   * while being perfectly sound at both ends of it - and a walk that used this transaction's pinned pages can be
   * following a chain that has since been rewritten. A clean walk here answers both: this is contention.</li>
   * <li>Are the chunks this read consumed still, byte for byte, the ones the newest committed image holds? This is
   * the guard the pre-existing {@link #isChunkChainBroken} probe never had: it is what rules out the half-published
   * commit, whose new pages would have moved the record under this read.</li>
   * </ol>
   * Only when the fresh walk is broken AND the read's own chunks are current is the record corrupted, and only then
   * does the read stop retrying and say so.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private String confirmBrokenChunkChain(final RID rid, final long[] chainTrace, final int chunks, final Binary record,
                                         final long lastNextChunkPointer, final long headMarker) {
    try {
      final String reason = findBrokenChunkChainInNewestCommitted(rid);
      if (reason == null)
        // THE COMMITTED CHAIN WALKS CLEANLY: what this read met was a chain in motion, which is what the retry is for.
        return null;

      return validateChainRead(chainTrace, chunks, record, lastNextChunkPointer, headMarker) == CHAIN_READ_CHANGED ?
              null :
              reason;
    } catch (final Exception e) {
      // Could not prove corruption (an I/O fault, most likely): fall back to the retry rather than condemn a record.
      LogManager.instance()
              .log(this, Level.FINE, "Unable to confirm whether the chunk chain of %s is broken", e, rid);
      return null;
    }
  }

  /**
   * The delete path's counterpart of {@link #confirmBrokenChunkChain}: confirms that a chunk chain
   * {@link #deleteRecordInternal} could not walk is genuinely broken rather than a record that MOVED under the
   * delete, and returns the reason it is broken, or {@code null} when the break cannot be told apart from
   * contention - in which case the caller keeps the retryable {@link ConcurrentModificationException} it always
   * raised (#6282, item 1).
   * <p>
   * The read path answers its second question with the bytes it consumed; a delete consumes none, so it asks a
   * sharper one instead: does the newest committed image fail at the SAME HOP, to the SAME TARGET? Two facts make
   * that the right question. A commit publishes its pages one at a time and readers take no lock, so a chain caught
   * mid-publication can look broken while being sound at both ends of it - and a clean walk here rules that out. And
   * a chain that is broken in the committed image but broken SOMEWHERE ELSE is not the chain this delete walked at
   * all: the record was rewritten under it, which is contention and must keep its retry rather than earn the
   * permanent verdict.
   * <p>
   * Only when both agree does the delete stop calling corruption contention. Everything else - an I/O fault while
   * confirming, a record that has since been deleted or rewritten as a single-page one - falls back to the retry
   * rather than condemn a record, because the verdict is not merely a message: it is non-retryable, and it points
   * the operator at {@code CHECK DATABASE FIX}, which DELETES the record.
   *
   * @param chunkId          index of the chunk whose continuation pointer the delete could not follow
   * @param nextChunkPointer the pointer it could not follow
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private String confirmBrokenChunkChainOnDelete(final RID rid, final int chunkId, final long nextChunkPointer) {
    try {
      final ChunkChainWalk walk = new ChunkChainWalk();
      final String reason = findBrokenChunkChainInNewestCommitted(rid, walk);
      if (reason == null)
        // THE COMMITTED CHAIN WALKS CLEANLY (or the record is no longer a chunked one): contention, which is what
        // the retry is for.
        return null;

      if (walk.brokenAtChunk != chunkId || walk.brokenAtPointer != nextChunkPointer)
        // BROKEN, BUT NOT WHERE THIS DELETE BROKE: a different chain, so a record that moved rather than a corrupt one.
        return null;

      return reason;
    } catch (final Exception e) {
      // Could not prove corruption (an I/O fault, most likely): fall back to the retry rather than condemn a record.
      LogManager.instance()
              .log(this, Level.FINE, "Unable to confirm whether the chunk chain of %s is broken", e, rid);
      return null;
    }
  }

  /**
   * {@link #findBrokenChunkChain} against the newest committed image of every page, transaction snapshot and all
   * page versions ignored. Returns {@code null} when the chain walks cleanly, and also when the record is no longer
   * there to walk (deleted, or no longer a chunked record): that is a record that changed, not a broken chain.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private String findBrokenChunkChainInNewestCommitted(final RID rid) throws IOException {
    return findBrokenChunkChainInNewestCommitted(rid, new ChunkChainWalk());
  }

  /**
   * @param walk collects the chunks the confirmation walk went through and WHERE it broke, so a caller can compare
   *             the committed image's break against the one it met itself rather than merely agree with it (#6282).
   */
  private String findBrokenChunkChainInNewestCommitted(final RID rid, final ChunkChainWalk walk) throws IOException {
    final int pageNumber = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);
    if (pageNumber >= getTotalPages())
      return null;

    final BasePage head = database.getPageManager()
            .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, true);
    if (head == null)
      return null;

    if (positionInPage >= head.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
      return null;

    final int recordPositionInPage = getRecordPositionInPage(head, positionInPage);
    if (recordPositionInPage == 0)
      // DELETED UNDER THE READ
      return null;

    if (!isChunkHead(head.readNumberAndSize(recordPositionInPage)[0]))
      // NO LONGER A MULTI-PAGE RECORD: the record was rewritten under the read.
      return null;

    return findBrokenChunkChain(rid, head, recordPositionInPage, true, walk);
  }

  /**
   * Whether {@code slot} of {@code page} still holds, byte for byte, the chunk a read consumed from it: the marker
   * that says which kind of chunk it is, the size it declares, the pointer it carries to the next one, and its
   * content - which lives in {@code record}, from {@code contentOffset}, because that is where the read copied it.
   * <p>
   * Never throws: a chunk that cannot be read where one was is a chunk that changed, which is the answer this
   * question wants for it anyway.
   */
  private boolean isChunkStillTheOneRead(final BasePage page, final int slot, final long expectedMarker,
                                         final int chunkSize, final long nextChunkPointer, final Binary record,
                                         final int contentOffset) {
    try {
      if (slot >= page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return false;

      final int chunkPositionInPage = getRecordPositionInPage(page, slot);
      if (chunkPositionInPage == 0)
        // The slot was freed: this chunk is no longer part of any record.
        return false;

      final long[] marker = page.readNumberAndSize(chunkPositionInPage);
      if (marker[0] != expectedMarker)
        return false;

      final int headerPos = (int) (chunkPositionInPage + marker[1]);
      if (page.readInt(headerPos) != chunkSize
              || page.readLong(headerPos + INT_SERIALIZED_SIZE) != nextChunkPointer)
        return false;

      final int contentPos = headerPos + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
      if (contentPos + chunkSize > page.getMaxContentSize())
        return false;

      // A chunk is up to a page long, so the content comparison is bulk (one intrinsified Arrays.equals over the two
      // heap arrays, stopping at the first difference) rather than a byte loop through the accessors.
      return page.isSameContentAs(contentPos, record, contentOffset, chunkSize);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to re-read chunk %d of page %s during a chunked read", e, slot,
              page.getPageId());
      return false;
    }
  }

  /**
   * @throws PageCorruptionException when the slot-table entry points inside the page header, which no writer can
   *                                 produce: the page was READ fine, its CONTENTS are nonsense. A distinct type
   *                                 rather than a plain {@link IOException} because a few lines away in the same
   *                                 chunk-chain walk an {@code IOException} means the exact opposite - the disk
   *                                 failed, so nothing at all is proved about the record (#6282).
   */
  private int getRecordPositionInPage(final BasePage page, final int positionInPage) throws IOException {
    final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
    if (recordPositionInPage != 0 && recordPositionInPage < contentHeaderSize)
      throw new PageCorruptionException(
          "Invalid record #" + fileId + ":" + recordPosition(page.pageId.getPageNumber(), maxRecordsInPage, positionInPage));
    return recordPositionInPage;
  }

  /**
   * Splits a record that does not fit its page into a chunk chain, writing the head chunk in the space the slot
   * already has on {@code currentPage} and the rest on other pages.
   *
   * @param firstChunkCoverage {@link MutablePage#COVERAGE_SLOT_MERGE} when the caller tracked this slot's images so
   *                           the disjoint-slot merge can replay the head chunk (#6129), or 0 when it did not - in
   *                           which case the caller has ALREADY poisoned this page. Only the writes landing on it are
   *                           declared: the chunks placed on the other pages stay undeclared, and those pages are
   *                           poisoned below.
   * @param slotBytesAlreadyOwned how many of {@code availableSpaceForFirstChunk} the slot already held (and the page
   *                           statistics already knew about) before this call: 0 for a record being created, and for
   *                           a record spilling out of its page its own footprint plus whatever the in-page shift
   *                           just claimed for it (#6149). The rest is free tail this write takes, which is what
   *                           {@link #updatePageStatistics} has to be told (#6154).
   * @param isPlaceHolderContent whether the record being written is the CONTENT of a placeholder rather than a record
   *                           of its own, which decides the marker its head chunk is stamped with (#6196). The
   *                           chain behind it is identical either way - only the head says which it is, and it is the
   *                           only place that information exists once the negated size marker is out of reach.
   */
  private void writeMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition,
                                    final int availableSpaceForFirstChunk, final int firstChunkCoverage,
                                    final int slotBytesAlreadyOwned, final boolean isPlaceHolderContent) throws IOException {
    // DISJOINT-SLOT MERGE (#5381): a multi-page write places chunk records onto pages via inline record-table
    // writes that bypass create/update/deleteRecordInternal, so it is NOT tracked. A chunk landing on a REUSED
    // page that also holds a tracked single-slot write would let the rebase re-derive that page from the tracked
    // slot alone and silently drop the chunk (corrupting the record). Poison every page this write touches so none
    // of them can be rebased. Poisoning a brand-new page is harmless (new pages are never rebase-tracked). The page
    // hosting the HEAD chunk is the caller's business (#6129): it either tracked the slot or poisoned the page.
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean poisonSlots = slotTx != null && slotTx.isSlotMergeEnabled();
    final MutablePage firstPage = currentPage;
    final int headChunkPageToAvoid = headChunkPageToAvoid(firstPage, firstChunkCoverage);

    int bufferSize = buffer.size();

    final int previousCoverage = currentPage.beginCoveredWrite(firstChunkCoverage);
    final int byteWritten;
    int chunkSize;
    try {
      // WRITE THE 1ST CHUNK
      byteWritten = currentPage.writeNumber(newPosition,
              isPlaceHolderContent ? FIRST_CHUNK_PLACEHOLDER_CONTENT : FIRST_CHUNK);

      newPosition += byteWritten;

      // WRITE CHUNK SIZE
      chunkSize = availableSpaceForFirstChunk - byteWritten - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
      currentPage.writeInt(newPosition, chunkSize);

      currentPage.writeByteArray(newPosition + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE, buffer.getContent(),
              buffer.getContentBeginOffset(), chunkSize);
    } finally {
      currentPage.endCoveredWrite(previousCoverage);
    }

    // #6154: the head chunk is the biggest bite anything takes out of a bucket page - a record spilling as the LAST
    // record of its page is given its own footprint PLUS the whole free tail, and a record being created is given the
    // tail outright - and it was the one chunk never fed into the free-space statistics, which then went on offering
    // the allocator a page with nothing left. Whatever the head chunk did not already own is tail it takes, and it
    // takes ALL of it whenever it takes any, so this lands the page's free space on exactly 0 and drops its entry.
    final int freeTailTakenByHeadChunk = availableSpaceForFirstChunk - slotBytesAlreadyOwned;
    if (freeTailTakenByHeadChunk > 0)
      updatePageStatistics(firstPage, freeTailTakenByHeadChunk, -freeTailTakenByHeadChunk);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    int txPageCounter = getTotalPages();
    while (bufferSize > 0) {
      MutablePage nextPage = null;
      int recordIdInPage = 0;

      final int spaceNeededForChunk = chunkFootprint(CHUNK_MARKER_BUDGET, bufferSize);

      final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), spaceNeededForChunk, txPageCounter,
              true, headChunkPageToAvoid);

      if (!pageAnalysis.createNewPage) {
        nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);

        if (nextPage.getVersion() != pageAnalysis.page.getVersion()) {
          // Page was modified by another committed transaction since the space analysis — skip reuse
          nextPage = null;
        } else {
          newPosition = pageAnalysis.newRecordPositionInPage;
          recordIdInPage = pageAnalysis.availablePositionIndex;

          // Verify the slot is still available on the actual mutable page (guard against stale analysis)
          final int existSlotOff = (int) nextPage.readUnsignedInt(
                  PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
          final short curRecCount = nextPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
          if (recordIdInPage < curRecCount && existSlotOff != 0) {
            // Slot was consumed by another operation — fall through to create a new page
            nextPage = null;
          } else {
            nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE, newPosition);

            if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
              nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
          }
        }
      }

      if (nextPage == null) {
        // CREATE A NEW PAGE
        // Atomically reserve the next page number to prevent two concurrent transactions
        // from allocating the same page number (which would cause silent data corruption
        // when their commits both succeed and overwrite each other's chunks).
        final int reservedPageNumber = reservedPageCounter.getAndIncrement();
        nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
        txPageCounter = reservedPageNumber + 1;
        newPosition = contentHeaderSize;
        nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
        nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
      }

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

      // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
      writeNextChunkPointer(currentPage, nextChunkPointerOffset,
              (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage,
              currentPage == firstPage ? firstChunkCoverage : 0);

      // The free tail this chunk is about to bite into, measured against the page's maximum content size - the same
      // base every other accounting in this class uses (#6358).
      final int freeSpaceBeforeChunk = nextPage.getMaxContentSize() - newPosition;

      final int chunkMarkerSize = nextPage.writeNumber(newPosition, NEXT_CHUNK);
      final int spaceAvailableInCurrentPage = freeSpaceBeforeChunk - chunkMarkerSize;

      // WRITE CHUNK SIZE
      chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
      final boolean lastChunk = bufferSize <= chunkSize;
      if (bufferSize < chunkSize)
        // LAST CHUNK, OVERWRITE THE SIZE WITH THE REMAINING CONTENT SIZE
        chunkSize = bufferSize;

      if (chunkSize < 1)
        throw new IllegalArgumentException("Chunk size invalid (" + chunkSize + ")");

      newPosition += chunkMarkerSize;
      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      nextChunkPointerOffset = newPosition + INT_SERIALIZED_SIZE;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      updatePageStatistics(nextPage, freeSpaceBeforeChunk, -chunkFootprint(chunkMarkerSize, chunkSize));

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }
  }

  /**
   * How many bytes a chunk occupies on the page that holds it: its marker, the {@code INT} declaring the content
   * size, the {@code LONG} pointing at the next chunk, and the content itself. The ONE derivation, called by all three
   * readers of a page's layout ({@link #contentEndOffset},
   * {@link #getOrderedRecordsInPage(BasePage, short, MutablePage)} - and therefore by the commit-time measurement of
   * {@link #accountCompressedPage}, which sums what the latter returned - and {@link #getPageOccupiedInBytes}) and by
   * every writer that places a chunk. Shared rather than merely agreed on, so the write path and the measurement path
   * cannot describe the same bytes differently again (#6358).
   * <p>
   * Before this existed each writer subtracted something of its own: the continuation chunk of a fresh spill left
   * the 12 header bytes out, the extension of an existing chain left the marker out and counted the whole remaining
   * buffer instead of the chunk it wrote, and the one landing on a brand-new page counted neither and measured
   * against a base that still included the page header. All three were absorbed by the thresholds in
   * {@link #updatePageStatistics} rather than by being right.
   *
   * @param markerSize bytes the chunk marker took, as {@code writeNumber} reported them.
   * @param chunkSize  content bytes the chunk carries, as its header declares them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static int chunkFootprint(final int markerSize, final int chunkSize) {
    return markerSize + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + chunkSize;
  }

  /**
   * The page the continuation chunks of a record must keep off, or -1 when there is none worth protecting (#6175).
   * <p>
   * It is the page holding the record's HEAD chunk, and only when the caller declared that slot replayable by the
   * disjoint-slot merge - which is exactly the case in which the page has something to lose. A continuation chunk
   * lands through inline record-table writes no slot image accounts for, so the page that receives one is poisoned
   * for the rest of the transaction; when the head chunk's page is the receiver, the poison falls on the one page
   * #6129 built its machinery (SLOT_KIND_FIRST_CHUNK, the chain fingerprint, the region re-derivation) to keep
   * mergeable, and it takes every unrelated record sharing that page down with it.
   * <p>
   * The reverse condition is just as deliberate: when the head chunk's slot was NOT declared (a record being created,
   * a spill onto an already-poisoned page), the page is unmergeable anyway and refusing its free tail would cost
   * space for nothing. That is also what keeps {@code createRecordInternal} allocating exactly as it did before -
   * there the head chunk is written at the page's content end and takes the whole free tail, so the question cannot
   * even arise.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static int headChunkPageToAvoid(final MutablePage headChunkPage, final int firstChunkCoverage) {
    return firstChunkCoverage == 0 ? -1 : headChunkPage.getPageId().getPageNumber();
  }

  /**
   * Writes the "pointer to the next chunk" field of a chunk header, declaring it covered by {@code coverage}. The
   * field lives inside the chunk's own slot image, so on the page holding the HEAD chunk of the record it is part of
   * what the disjoint-slot merge replays (#6129); on any other page the caller passes 0 and the write is as
   * undeclared as it was before, which is what keeps those pages out of every merge.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static void writeNextChunkPointer(final MutablePage page, final int offset, final long pointer, final int coverage) {
    final int previousCoverage = page.beginCoveredWrite(coverage);
    try {
      page.writeLong(offset, pointer);
    } finally {
      page.endCoveredWrite(previousCoverage);
    }
  }

  /**
   * Rewrites a record that is already stored as a chunk chain, reusing the chain it has and extending, shrinking or
   * relocating it as the new content requires.
   *
   * @param firstChunkCoverage {@link MutablePage#COVERAGE_SLOT_MERGE} when the caller tracked the head chunk's slot
   *                           image so the disjoint-slot merge can replay this page's part of the write (#6129), or 0
   *                           when it did not - in which case the caller has ALREADY poisoned the head chunk's page.
   *                           Only the writes landing on that page are declared: every other page of the chain is
   *                           poisoned below and its writes stay undeclared, so neither merge can re-derive it.
   * @param headChunkRegionEnd first byte after the room the head chunk may use on this page, as
   *                           {@link #headChunkRegionEnd} derives it from the page's own slot table.
   */
  private void updateMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition,
                                     final int firstChunkCoverage, final int headChunkRegionEnd) throws IOException {
    // DISJOINT-SLOT MERGE (#5381): like writeMultiPageRecord, this rewrites/relocates/frees chunk records on
    // existing pages through inline record-table writes that bypass the tracking hooks. Poison every page it
    // touches so a page carrying both a tracked single-slot write and a chunk write can never be rebased. The page
    // holding the HEAD chunk is the exception (#6129): the writes this method makes to it stay inside the head
    // chunk's own bytes, which the caller tracked - unless the chain comes back to it, and then the loops below
    // poison it like any other page.
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean poisonSlots = slotTx != null && slotTx.isSlotMergeEnabled();
    final MutablePage firstPage = currentPage;
    final int headChunkPageToAvoid = headChunkPageToAvoid(firstPage, firstChunkCoverage);

    final int previousChunkSize = currentPage.readInt(newPosition);
    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK, USING ALL THE ROOM THE HEAD CHUNK OWNS ON THIS PAGE
    //
    // #6163: sized from the PAGE, not from what the chunk header happens to declare today. Deriving it from the
    // header could only ever cap it downwards - a shrink rewrote the declared size and no path ever raised it again -
    // so a record that oscillated in size pinned its head chunk to the smallest size it had ever had and pushed the
    // difference into continuation chunks on other pages for the rest of its life. The room is the same one a fresh
    // spill would be given (writeMultiPageRecord's availableSpaceForFirstChunk), which is what makes an update place
    // its head chunk exactly as a spill of the new content would have.
    final int firstChunkContentOffset = newPosition + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
    final int headChunkRoom = headChunkRegionEnd - firstChunkContentOffset;

    // The two ways the region can disagree with the chunk it holds are NOT the same failure, and are answered
    // differently on purpose - the fatal one first, so it is the one that speaks when both are true.
    //
    // A region that cannot host a single byte of content is where the arithmetic stops being safe: a chunk size of 0
    // or below is not a shorter chunk, it is a header no reader can walk. Refused outright, in the same terms
    // writeMultiPageRecord refuses an invalid continuation chunk. Reaching it means the page is corrupted (a
    // neighbour's slot pointing inside this chunk's own header), and failing the transaction beats writing a header
    // that would make the record unreadable.
    if (headChunkRoom < 1)
      throw new DatabaseOperationException(
              "Head chunk of record " + originalRID + " has no room left in page " + currentPage.getPageId() + " (region ends at "
                      + headChunkRegionEnd + ", chunk content starts at " + firstChunkContentOffset + ")");

    // A region merely SMALLER than what the head chunk declares breaks an invariant of a healthy page (nothing may
    // start inside another record's bytes), but it cannot corrupt anything from here: the head chunk is written
    // shorter and the content that no longer fits leaves through the continuation loop below, which is the ordinary
    // path. That is an assertion's job - loud in the tests (surefire runs with -ea), harmless in production.
    assert previousChunkSize <= headChunkRoom :
        "the head chunk of " + originalRID + " declares " + previousChunkSize + " bytes in a region of " + headChunkRoom;

    int chunkSize = Math.min(bufferSize, headChunkRoom);

    final int previousCoverage = currentPage.beginCoveredWrite(firstChunkCoverage);
    try {
      currentPage.writeInt(newPosition, chunkSize);
      currentPage.writeByteArray(firstChunkContentOffset, buffer.getContent(), buffer.getContentBeginOffset(), chunkSize);
    } finally {
      currentPage.endCoveredWrite(previousCoverage);
    }

    // #6154: the head chunk moves the page's free tail - the only chunk that can - and only when it is the page's
    // LAST record, which is exactly when its region runs to the maximum content size. In every other case the region
    // is bounded by the neighbour that follows and the tail does not move, so there is nothing to account.
    if (headChunkRegionEnd == currentPage.getMaxContentSize() && chunkSize != previousChunkSize)
      updatePageStatistics(currentPage, headChunkRoom - previousChunkSize, previousChunkSize - chunkSize);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;
    long nextChunkPointer = currentPage.readLong(nextChunkPointerOffset);

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    long chunkToDeletePointer = bufferSize > 0 ? 0L : nextChunkPointer;

    while (bufferSize > 0) {
      MutablePage nextPage = null;

      // Only a chunk placed on space just TAKEN from a page moves that page's free tail; a chunk rewritten inside
      // the footprint the chain already owns moves nothing. -1 says "nothing to account", and the two below carry
      // the measurement to the single accounting at the end of the iteration, where the chunk's size is settled
      // (#6358) - before this, the two allocating branches each guessed at it from what they knew before the write.
      int freeSpaceBeforeChunk = -1;
      int chunkMarkerSize = 0;
      // #6396: what the reused chunk declared before this iteration rewrote it, so a chunk that comes out SHORTER
      // can say so. -1 while there is no reused chunk.
      int chunkSizeBefore = -1;

      if (nextChunkPointer > 0) {
        final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

        nextPage = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
        final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
        if (recordPositionInPage == 0)
          // Chunk was deleted by a concurrent update to the same record — signal retry
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " chunk was modified concurrently. Please retry the operation");

        final long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

        if (recordSize[0] != NEXT_CHUNK)
          // Chunk was overwritten by a concurrent operation — signal retry
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " chunk was modified concurrently. Please retry the operation");

        newPosition = (int) (recordPositionInPage + recordSize[1]);
        chunkSize = nextPage.readInt(newPosition);
        chunkSizeBefore = chunkSize;
        nextChunkPointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);

      } else {
        // CREATE NEW SPACE FOR THE CURRENT AND REMAINING CHUNKS
        int recordIdInPage = 0;
        int txPageCounter = getTotalPages();

        // #6358: budgeted for the marker too. Leaving it out asked for one byte less than the chunk goes on to
        // occupy, so a page with exactly that much left was accepted and the chunk written on it came out a byte
        // shorter than the request implied - harmless, and one more way this call site described a chunk differently
        // from the one in writeMultiPageRecord that asks the same question.
        final int totalSpaceNeeded = chunkFootprint(CHUNK_MARKER_BUDGET, bufferSize);

        final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), totalSpaceNeeded, txPageCounter,
                true, headChunkPageToAvoid);
        if (!pageAnalysis.createNewPage) {
          nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);

          if (nextPage.getVersion() != pageAnalysis.page.getVersion()) {
            // Page was modified by another committed transaction since the space analysis — skip reuse
            nextPage = null;
          } else {
            newPosition = pageAnalysis.newRecordPositionInPage;
            recordIdInPage = pageAnalysis.availablePositionIndex;

            // Verify the slot is still available on the actual mutable page (guard against stale analysis)
            final int existSlotOff2 = (int) nextPage.readUnsignedInt(
                    PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
            final short curRecCount2 = nextPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
            if (recordIdInPage < curRecCount2 && existSlotOff2 != 0) {
              // Slot was consumed by another operation — fall through to create a new page
              nextPage = null;
            } else {
              nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pageAnalysis.availablePositionIndex * INT_SERIALIZED_SIZE,
                      newPosition);

              if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
                nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
            }
          }
        }

        if (nextPage == null) {
          // CREATE A NEW PAGE
          // Atomically reserve the next page number to prevent two concurrent transactions
          // from allocating the same page number (which would cause silent data corruption).
          final int reservedPageNumber = reservedPageCounter.getAndIncrement();
          nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
          newPosition = contentHeaderSize;
          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
        }

        // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
        writeNextChunkPointer(currentPage, nextChunkPointerOffset,
                (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage,
                currentPage == firstPage ? firstChunkCoverage : 0);

        // Both branches above - a page the allocator reused and a page created for this chunk - hand out the free
        // tail starting at newPosition, so the base is the same one and it is the page's maximum content size, never
        // getAvailableContentSize() (which still counts the page header, the overstatement #4958 and #5067 removed
        // from everywhere else).
        freeSpaceBeforeChunk = nextPage.getMaxContentSize() - newPosition;

        chunkMarkerSize = nextPage.writeNumber(newPosition, NEXT_CHUNK);
        final int spaceAvailableInCurrentPage = freeSpaceBeforeChunk - chunkMarkerSize;

        chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
        newPosition += chunkMarkerSize;
      }

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

      // WRITE CHUNK SIZE
      final boolean lastChunk = bufferSize <= chunkSize;
      if (lastChunk) {
        // LAST CHUNK: everything past it is chain the record no longer needs, and its pointer is what frees it below.
        //
        // #6319: the two facts have to be established by the SAME condition, and for a long time they were not - the
        // chain was cut whenever the content ended AT OR BEFORE this chunk, but the tail was handed to freeChunkChain
        // only when it ended strictly BEFORE it. Content that ends exactly on a chunk boundary therefore cut the chain
        // and freed nothing: the chunks past the cut kept their slots and their pointers to each other, reachable from
        // no record, for the life of the bucket. Far from a corner case - a record whose chunks were grown one field
        // at a time and then loses a field lands on a boundary by construction, because the boundary is where that
        // field's own bytes were appended. Measured on CRUDTest.multiUpdatesOverlap: every single record that stayed a
        // chain across the shrink round leaked its whole tail, 243821 orphaned chunks out of 1545495.
        chunkSize = bufferSize;
        chunkToDeletePointer = nextChunkPointer;
      }

      nextPage.writeInt(newPosition, chunkSize);

      // #6358: the chunk's footprint is known only here - the last-chunk branch above may have shortened it - and
      // it is one derivation shared with the spill and with the page walk, rather than a subtraction each writer
      // improvised.
      if (freeSpaceBeforeChunk > -1)
        updatePageStatistics(nextPage, freeSpaceBeforeChunk, -chunkFootprint(chunkMarkerSize, chunkSize));
      else if (chunkSize < chunkSizeBefore)
        // #6396: the LAST chunk of a record that shrank is rewritten shorter inside the footprint the chain already
        // owns. It takes nothing, so there is no delta to report - but when that chunk is the last record of its
        // page it hands bytes back to the free tail, which is the compression's to measure and no longer anything a
        // previous claim on this page describes.
        freedWithoutClaiming(nextPage);

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
    if (chunkToDeletePointer > 0) {
      // CUT THE CHAIN AT THE LAST CHUNK THAT SURVIVES, THEN FREE WHAT FOLLOWS
      writeNextChunkPointer(currentPage, nextChunkPointerOffset, 0L, currentPage == firstPage ? firstChunkCoverage : 0);
      freeChunkChain(originalRID, chunkToDeletePointer);
    }
  }

  /**
   * Turns the head chunk of a multi-page record back into a PLAIN record when the new content fits the region that
   * slot owns on its own page, freeing the whole chain behind it (#6178). The inverse of the spill, and the last of
   * the three transitions between the two shapes.
   * <p>
   * The bound is the region and nothing else - the same one {@link #headChunkRegionEnd} derives for the head chunk
   * itself - so a record that shrank a long way back but still does not fit its slot stays a chain rather than
   * claiming bytes that belong to its neighbours. That makes this reachable in exactly two ways, both real: a record
   * that spilled as the LAST record of its page owns the whole free tail and can come back from a very long way, and
   * a record in the middle of its page owns its own footprint and comes back when it returns to roughly the size it
   * had before it grew.
   * <p>
   * What is written here is one slot - the size marker and the content - inside the region, which is what makes it a
   * single-slot write the merge can replay. The chain, on the other pages, is freed and those pages poisoned, exactly
   * as an ordinary shrink already does with the chunks it drops.
   * <p>
   * It also means a chunk head that declares LESS than its region does not survive an update: content that fits the
   * head chunk (13 bytes of header) fits the region as a plain record (at most 5 bytes of size marker), so the two
   * conditions coincide and the chain-of-one-chunk is collapsed instead of being kept. That is the same state #6163's
   * grow-back exists to repair, which this removes at the source rather than compensating for.
   * <p>
   * #6286: the CONTENT record of a placeholder collapses here too, into the NEGATED size marker its shape is
   * recognised by rather than into a plain positive one. It was left out when this was written because the collapse
   * could only write a positive marker, and a positive marker is exactly what tells {@code scan()} and {@code check()}
   * that a slot holds a document of its own - so such a record kept its chain however far it shrank, 13 bytes of
   * header for ever and every read and write on the chunk path, on the one record shape that needs a placeholder to
   * exist at all. Writing the sign the shape calls for is all that was missing, and it makes this the mirror of the
   * spill for BOTH kinds of head chunk instead of only one.
   *
   * @param chunkBaseImage      the head chunk's pre-image when the caller found the slot rebasable, or {@code null}
   *                            when it did not - in which case it has already poisoned the page and this only writes.
   *                            When it is not null, {@code slotTx} is not null either: the caller could not have read
   *                            it.
   * @param isPlaceHolderContent whether this slot is the CONTENT of a placeholder POINTER rather than a record of its
   *                            own. Decided by the caller from the FLAG it was reached through and not from the marker
   *                            found on the page, because a database written before #6196 holds a content head still
   *                            wearing the ambiguous {@link #FIRST_CHUNK}.
   *
   * @return true when the record was collapsed and there is nothing left for the caller to write.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private boolean collapseChunkChainToRecord(final RID rid, final Binary buffer, final MutablePage page, final int pageId,
                                             final int positionInPage, final int recordPositionInPage, final int chunkHeaderPos,
                                             final int chunkRegionEnd, final byte[] chunkBaseImage,
                                             final boolean isPlaceHolderContent, final TransactionContext slotTx)
          throws IOException {
    final int bufferSize = buffer.size();
    // #6286: which shape the slot collapses INTO. A record of its own gets the plain positive size marker; the CONTENT
    // record of a placeholder gets that size NEGATED, the same marker every content record small enough for its page
    // has always carried - which is what keeps a scan from handing these bytes out as a document of their own.
    final long sizeMarker = isPlaceHolderContent ? -1L * bufferSize : bufferSize;
    final int sizeBytes = Binary.getNumberSpace(sizeMarker);
    if (recordPositionInPage + sizeBytes + bufferSize > chunkRegionEnd)
      // Still too big for its own slot: it stays a chunk chain.
      return false;

    // Read BEFORE the write below overwrites the header these two come from.
    final long chainToFree = page.readLong(chunkHeaderPos + INT_SERIALIZED_SIZE);
    final int previousChunkEnd = chunkHeaderPos + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + page.readInt(chunkHeaderPos);

    final int previousCoverage = page.beginCoveredWrite(chunkBaseImage != null ? MutablePage.COVERAGE_SLOT_MERGE : 0);
    try {
      final int sizeBytesWritten = page.writeNumber(recordPositionInPage, sizeMarker);
      // The offset the body is placed at was budgeted with getNumberSpace above, and writeNumber is the only other
      // place that decides how many bytes a size takes. They agree by construction; a future divergence would write
      // the body past the room just proved - a silent overwrite of the next record rather than a refusal, which is
      // exactly what this assertion exists to make loud under -ea (surefire's default).
      assert sizeBytesWritten == sizeBytes :
          "the record size of " + rid + " took " + sizeBytesWritten + " bytes where " + sizeBytes + " were budgeted";
      page.writeByteArray(recordPositionInPage + sizeBytesWritten, buffer.getContent(), buffer.getContentBeginOffset(),
              bufferSize);
    } finally {
      page.endCoveredWrite(previousCoverage);
    }

    // #6154: the same accounting an ordinary shrink of the head chunk makes, and for the same reason - only a slot
    // whose region runs to the page's maximum content size moves the free tail; anywhere else the region is bounded
    // by the neighbour that follows and nothing moves.
    if (chunkRegionEnd == page.getMaxContentSize())
      updatePageStatistics(page, chunkRegionEnd - previousChunkEnd,
              previousChunkEnd - (recordPositionInPage + sizeBytes + bufferSize));

    freeChunkChain(rid, chainToFree);

    // freeChunkChain may have poisoned this very page (a chunk of the chain lived on it): re-check before tracking.
    if (chunkBaseImage != null && !slotTx.isSlotRebasePagePoisoned(fileId, pageId))
      slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, chunkBaseImage,
              Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(), buffer.getContentBeginOffset() + bufferSize),
              isPlaceHolderContent ?
                      TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT :
                      TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD);

    LogManager.instance().log(this, Level.FINE,
            "Updated record %s by collapsing its chunk chain back into a plain %srecord (%s threadId=%d)", null, rid,
            isPlaceHolderContent ? "placeholder content " : "", page, Thread.currentThread().getId());

    return true;
  }

  /**
   * Frees every chunk of a chain, starting at {@code chunkPointer}, by zeroing its slot-table entry on the page that
   * holds it. Poisons every page it visits: the freed slot is a change no tracked slot image accounts for, so a page
   * that also carries a tracked write could otherwise be re-derived from that write alone and quietly resurrect the
   * chunk. Never throws on a chain it cannot walk - a broken pointer is logged and the walk stops there, exactly as
   * it did when this was inlined in {@link #updateMultiPageRecord} (the record's own slot has already been rewritten
   * by the caller, so the chunks left behind are unreachable rather than dangerous, and {@code check(fix=true)}
   * collects them).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void freeChunkChain(final RID originalRID, long chunkPointer) throws IOException {
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean poisonSlots = slotTx != null && slotTx.isSlotMergeEnabled();

    while (chunkPointer > 0) {
      final int chunkPageId = (int) (chunkPointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (chunkPointer % maxRecordsInPage);

      final MutablePage nextPage = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
      final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

      // DELETE THE CHUNK AS RECORD IN THE PAGE
      nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE, 0L);
      freedWithoutClaiming(nextPage);

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

      // #6339: nothing is told to the free-space statistics here, deliberately. The bytes are the page's again, but
      // the chunk leaves a HOLE that the allocator cannot hand out, and what the page will really have on offer is
      // settled by the compression the commit runs on it (compressPageInternal, which reports what it packed). A
      // guess made here would have to be undone there.
      chunkPointer = nextPage.readLong((int) (recordPositionInPage + recordSize[1]) + INT_SERIALIZED_SIZE);
    }
  }

  /**
   * Where the region a HEAD chunk may use on its own page ENDS: the content offset of the record stored right after
   * it, or the page's maximum content size when nothing is (which is exactly the case of a chunk head that is the
   * page's LAST record). Everything in between belongs to the head chunk and to nothing else - the allocator only
   * ever hands out the page's tail, and a record that is not the last one can only be followed by its neighbour or by
   * the dead bytes of a delete, which no other record can claim either.
   * <p>
   * This is the room {@code writeMultiPageRecord} was given when the record spilled, re-derived from the page instead
   * of remembered, and it is what makes an update size its head chunk exactly as a fresh spill would (#6163). It is
   * derived, never stored, for a reason: a chunk header declares its CONTENT length, not its footprint, so the room a
   * shrink left unused is invisible in the record's own bytes; and the page can hand it away meanwhile
   * ({@code compressPage} closes the hole, an insert takes the released tail), after which this returns the smaller
   * region and the head chunk simply stops there.
   * <p>
   * The slot-table scan is the same one {@link #getLastRecordPositionInPage} already costs the growth path on every
   * update that does not fit in place, against a write that then moves kilobytes of chunk content: there is nothing
   * to cache here, because the answer is exactly the page state a concurrent commit is allowed to change.
   *
   * @param recordPositionInPage content offset of the head chunk's slot, marker included.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private int headChunkRegionEnd(final BasePage page, final short recordCountInPage, final int recordPositionInPage)
          throws IOException {
    int regionEnd = page.getMaxContentSize();
    for (int pos = 0; pos < recordCountInPage; ++pos) {
      final int nextRecordPositionInPage = getRecordPositionInPage(page, pos);
      if (nextRecordPositionInPage > recordPositionInPage && nextRecordPositionInPage < regionEnd)
        regionEnd = nextRecordPositionInPage;
    }
    return regionEnd;
  }

  /**
   * The byte at which a bucket page's used content ends: the position right after the last record it stores, and therefore
   * the start of its free tail. Resolves the real size of that last record for every marker shape it can have (plain
   * record, placeholder pointer, placeholder content, multi-page chunk).
   *
   * @param lastRecordPositionInPage highest content offset in use in the page (see {@link #getLastRecordPositionInPage}).
   * @param recordPositionInPage     content offset of the record being written, whose size the caller already resolved
   *                                 in {@code recordSize} - avoids reading it twice when it IS the last one.
   */
  private int getPageOccupiedInBytes(final MutablePage page, final int lastRecordPositionInPage,
                                     final int recordPositionInPage, final long[] recordSize) throws IOException {
    final long[] lastRecordSize;
    if (lastRecordPositionInPage != recordPositionInPage) {
      // CURRENT RECORD IS NOT THE LATEST RIGHT IN THE PAGE

      if (lastRecordPositionInPage < contentHeaderSize)
        // IT SHOULD NEVER OCCUR BECAUSE THE CURRENT RECORD IS PRESENT IN THIS PAGE
        throw new DatabaseOperationException(
                "Invalid position " + lastRecordPositionInPage + " on expanding a record of page " + page.getPageId());

      lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);

      if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER) {
        lastRecordSize[0] = LONG_SERIALIZED_SIZE;
        lastRecordSize[1] = 1L;
      } else if (isChunkHead(lastRecordSize[0]) || lastRecordSize[0] == NEXT_CHUNK) {
        // #6358: the chunk's footprint through the one derivation, and against the marker size readNumberAndSize
        // actually reported rather than a hardcoded 1. The 1 was right for every marker in use - they all encode to a
        // single byte - which is exactly the kind of agreement-by-coincidence the shared helper exists to replace:
        // this is the THIRD reader of a page's layout, and what it returns feeds updatePageStatistics through the
        // spill's slot enlargement and through growRecordInPage.
        final int chunkMarkerSize = (int) lastRecordSize[1];
        return lastRecordPositionInPage + chunkFootprint(chunkMarkerSize,
                page.readInt(lastRecordPositionInPage + chunkMarkerSize));
      } else if (lastRecordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        lastRecordSize[0] *= -1L;
      }
    } else
      lastRecordSize = recordSize;

    return (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]);
  }

  /**
   * Writes a record that no longer fits its own space INSIDE its page, shifting the content that follows it to the
   * right and fixing the slot offsets of the records that moved. Only the grown record's content changes: every other
   * slot keeps its record, merely at a different offset that is recomputed from the page - which is what makes the
   * write replayable on a newer committed version of the page by the disjoint-slot merge (#5279,
   * {@link #rebaseRecordOnPage}).
   * <p>
   * Shared by the update path and by that commit-time replay, so both grow a record exactly the same way.
   *
   * @param recordSize the {size, sizeLength} pair of the record as currently stored, as returned by
   *                   {@link MutablePage#readNumberAndSize}; {@code recordSize[1]} is updated on a successful write.
   *
   * @return true when the page could host the extra bytes and the record was written; false when the caller has to
   * spill the record out of the page (placeholder or chunk chain) - in that case nothing has been written.
   */
  private boolean growRecordInPage(final MutablePage page, final short recordCountInPage,
                                   final int recordPositionInPage, final int lastRecordPositionInPage, final long[] recordSize,
                                   final boolean isPlaceHolder, final byte[] content, final int contentOffset,
                                   final int contentSize) throws IOException {
    final int pageOccupiedInBytes = getPageOccupiedInBytes(page, lastRecordPositionInPage, recordPositionInPage, recordSize);
    final int spaceAvailableInCurrentPage = page.getMaxContentSize() - pageOccupiedInBytes;
    final int contentSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * contentSize : contentSize);
    final int additionalSpaceNeeded = (int) (contentSize + contentSizeLength - recordSize[0] - recordSize[1]);

    if (additionalSpaceNeeded >= spaceAvailableInCurrentPage)
      // NOT ENOUGH ROOM LEFT IN THE PAGE
      return false;

    // THERE IS SPACE LEFT IN THE PAGE, SHIFT ON THE RIGHT THE EXISTENT RECORDS
    if (lastRecordPositionInPage != recordPositionInPage)
      // NOT LAST RECORD IN PAGE, SHIFT NEXT RECORDS
      shiftFollowingRecordsRight(page, recordCountInPage, (int) (recordPositionInPage + recordSize[0] + recordSize[1]),
              pageOccupiedInBytes, additionalSpaceNeeded);

    recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * contentSize : contentSize);
    page.writeByteArray((int) (recordPositionInPage + recordSize[1]), content, contentOffset, contentSize);

    updatePageStatistics(page, spaceAvailableInCurrentPage, -additionalSpaceNeeded);
    return true;
  }

  /**
   * How many bytes the in-page shift has to claim so a slot holding {@code roomAlreadyAvailable} bytes can host a
   * chunk head of {@code roomRequired} (#6149). The single place that decision is made: the spill in
   * {@link #updateRecordInternal} asks for {@link #MINIMUM_SPACE_FOR_FIRST_CHUNK}, its commit-time replay in
   * {@link #rebaseRecordOnPage} asks for the exact size of the head chunk it has to put back, and because both go
   * through here they cannot drift apart on the margin or on the last-record rule - which is what lets the replay
   * reproduce the write's shift rather than merely resemble it.
   * <p>
   * Only the MISSING bytes are ever claimed: a head chunk's footprint is fixed for the life of the record
   * ({@code updateMultiPageRecord} rewrites only inside it), so a bigger bite would deny page space to every other
   * record on the page for good.
   *
   * @param roomRequired         bytes the chunk head needs in total, marker included.
   * @param roomAlreadyAvailable bytes the slot may already use: its own footprint, plus the page's free tail when it
   *                             is the last record.
   * @param freeTailInPage       unused bytes between the end of the page's last record and its maximum content size.
   * @param isLastRecordInPage   whether the slot is the last record of the page. Then there is nothing after it to
   *                             move, and its free tail is already counted in {@code roomAlreadyAvailable}.
   *
   * @return 0 when the slot is already big enough and no shift is needed; a positive count of bytes to shift by; or
   * -1 when the page cannot provide them, which sends the caller to the placeholder fallback (write) or to a plain
   * retry (replay). The margin is the one {@link #growRecordInPage} keeps - the shift must leave at least one spare
   * byte - so a growth and a spill judge the same page identically.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static int slotEnlargementForChunkHead(final int roomRequired, final int roomAlreadyAvailable,
                                                 final int freeTailInPage, final boolean isLastRecordInPage) {
    if (roomAlreadyAvailable >= roomRequired)
      return 0;

    final int missing = roomRequired - roomAlreadyAvailable;
    if (isLastRecordInPage || missing >= freeTailInPage)
      return -1;

    return missing;
  }

  /**
   * Moves everything stored from {@code from} onwards {@code additionalSpaceNeeded} bytes to the right and fixes the
   * slot-table offsets of the records that moved, enlarging by that much the footprint of the record that ends at
   * {@code from}. Every other slot keeps its record - and therefore its RID, which is the slot index and not the
   * offset - so this is a single-slot change as far as the disjoint-slot merge is concerned (#5279): the replay
   * re-does it on the newer committed page from that page's own slot table.
   * <p>
   * The caller must have checked the page has the bytes ({@code additionalSpaceNeeded} strictly below the free tail)
   * and must declare the write coverage: this method only moves bytes.
   *
   * @param page                  the page to re-flow.
   * @param recordCountInPage     number of slot-table entries to walk when fixing the offsets that moved.
   * @param from                  first byte to move: the end of the growing record's current footprint.
   * @param pageOccupiedInBytes   the byte at which the page's used content ends (see {@link #getPageOccupiedInBytes}).
   * @param additionalSpaceNeeded bytes to move by, which the caller has proved the page's free tail can absorb.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private void shiftFollowingRecordsRight(final MutablePage page, final short recordCountInPage, final int from,
                                          final int pageOccupiedInBytes, final int additionalSpaceNeeded) throws IOException {
    page.move(from, from + additionalSpaceNeeded, pageOccupiedInBytes - from);

    // TODO: CALCULATE THE REAL SIZE TO COMPACT DELETED RECORDS/PLACEHOLDERS
    for (int pos = 0; pos < recordCountInPage; ++pos) {
      final int nextRecordPosInPage = getRecordPositionInPage(page, pos);
      if (nextRecordPosInPage != 0 &&//
              nextRecordPosInPage >= from &&//
              nextRecordPosInPage <= pageOccupiedInBytes) {
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE,
                nextRecordPosInPage + additionalSpaceNeeded);

        // The invariant this states is about the records that MOVED: each one must still start inside the page.
        // It used to sit outside this branch, where it also ran for every slot left alone - vacuously true for them
        // (their offset is below `from`, and a freed slot reads 0), so it asserted nothing while looking like it
        // did. Harmless while one method owned it; worth stating precisely now that three call sites share it.
        assert nextRecordPosInPage + additionalSpaceNeeded < page.getMaxContentSize();
      }
    }
  }

  /**
   * Highest content offset in use in {@code page}, or -1 when no slot below {@code totalRecords} holds one. A 0
   * entry in the record table is a hole left by a delete, not a record at content offset 0 (the page header lives
   * there), so it is skipped - see {@link #contentEndOffset}.
   */
  private int getLastRecordPositionInPage(final BasePage page, final int totalRecords) throws IOException {
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
        final RID potentialRID = new RID(file.getFileId(),
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

    pageAnalysis.newRecordPositionInPage = contentEndOffset(pageAnalysis.page, pageAnalysis.lastRecordPositionInPage);
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
   * @param avoidPageNumber a page this allocation may NOT use, or -1 for none (#6175). Used by the continuation
   *                        chunks of a multi-page record to keep off the page holding that record's HEAD chunk when
   *                        the caller tracked that slot for the disjoint-slot merge: a continuation chunk is written
   *                        through inline record-table writes no slot image accounts for, so it poisons the page it
   *                        lands on - and that page is the one #6129 went to some trouble to keep mergeable, since a
   *                        chunked record's update is a single-slot write on it. The locality the same-page
   *                        preference buys is worth little to a chunk that is only ever reached by pointer, from a
   *                        page that is already loaded; the poisoning costs every OTHER record of that page its
   *                        merge for the whole transaction.
   */
  private PageAnalysis findAvailableSpace(final int currentPageId, final int spaceNeeded, final int txPageCounter,
                                          final boolean multiPageRecord, final int avoidPageNumber)
          throws IOException {
    if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.MEDIUM.ordinal()) {
      synchronized (freeSpaceInPages) {
        if (freeSpaceInPages.isEmpty())
          gatherPageStatistics();

        // TRY WITH THE CURRENT PAGE FIRST
        PageAnalysis bestPageAnalysis = null;

        if (currentPageId > -1 && currentPageId != avoidPageNumber) {
          // PRIORITIZE SPACE IN THE SAME PAGE
          bestPageAnalysis = getAvailableSpaceInPage(currentPageId, spaceNeeded, multiPageRecord);
          if (bestPageAnalysis.createNewPage || bestPageAnalysis.totalRecordsInPage > maxRecordsInPage)
            bestPageAnalysis = null;
        }

        if (bestPageAnalysis == null) {
          if (freeSpaceInPages.isEmpty())
            gatherPageStatistics();

          if (!freeSpaceInPages.isEmpty()) {
            bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded, multiPageRecord,
                    avoidPageNumber);
            if (bestPageAnalysis == null)
              // TRY AGAIN WITH HALF SIZE, THE RECORD WILL BE SPLIT IN MULTIPLE CHUNKS
              bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded / 2, multiPageRecord,
                      avoidPageNumber);
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
    final PageAnalysis lastPageAnalysis = getAvailableSpaceInPage(txPageCounter - 1, spaceNeeded, multiPageRecord);
    if (txPageCounter - 1 == avoidPageNumber)
      // The last page is the one page this allocation may not use, so a brand-new page is all that is left - and a
      // brand-new page is one no other record is on, so poisoning it costs nobody anything.
      lastPageAnalysis.createNewPage = true;
    return lastPageAnalysis;
  }

  /**
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   * @param avoidPageNumber a page this allocation may NOT use, or -1 for none: see
   *                        {@link #findAvailableSpace(int, int, int, boolean, int)}.
   */
  private PageAnalysis findAvailableSpaceFromStatistics(final int currentPageId, final int spaceNeeded,
                                                        final boolean multiPageRecord, final int avoidPageNumber)
          throws IOException {
    // Snapshot keys/values into local arrays so we can safely mutate freeSpaceInPages
    // (put/remove) inside the loop body. The snapshot is bounded by MAX_PAGES_GATHER_STATS (100).
    //
    // The snapshot is sorted by pageId to preserve the deterministic "fill lowest pageId first" allocation behaviour
    // that the previous TreeMap-backed implementation provided implicitly via its ordered iteration. What it buys is
    // a REPRODUCIBLE choice among the candidate pages - the same bucket state allocates the same way twice - and the
    // locality of preferring the pages nearest the front of the file.
    //
    // What it does NOT buy, and used to be claimed here (#6339): that a re-insert after a bulk delete lands on the
    // RID the original insert had. It cannot, and it could not then either - which page is a candidate at all depends
    // on what the free-space statistics hold at that moment, and this ordering only decides between the pages that
    // made it into that map. RandomDeleteTest was cited as depending on it and no longer does: it compares the scan
    // against the records it inserted rather than against the order it inserted them in.
    final int snapSize = freeSpaceInPages.size();
    final int[] snapPageIds = new int[snapSize];
    final int[] snapPageStats = new int[snapSize];
    final int[] cursor = { 0 };
    freeSpaceInPages.forEach((k, v) -> {
      snapPageIds[cursor[0]] = k;
      snapPageStats[cursor[0]] = v;
      cursor[0]++;
    });
    sortByPageIdAscending(snapPageIds, snapPageStats, snapSize);

    PageAnalysis bestPageAnalysis = null;
    int[] pagesToRemove = null;
    int pagesToRemoveCount = 0;
    // Visible page horizon: the tx can see committed pages PLUS any pages it has added itself
    // (tracked via tx.getPageCounter on this file). Pages beyond this are "phantoms" reserved
    // by other concurrent transactions, not yet committed and not visible via readCache; reusing
    // them at slot 0 would collide with the reserving tx and silently corrupt that record's chain.
    final int txVisiblePageHorizon = getTotalPages();
    for (int s = 0; s < snapSize; s++) {
      final int pageId = snapPageIds[s];
      if (pageId == currentPageId)
        // ALREADY EVALUATED
        continue;

      if (pageId == avoidPageNumber)
        // OFF LIMITS FOR THIS ALLOCATION (#6175)
        continue;

      if (pageId >= txVisiblePageHorizon)
        continue;

      final int pageStats = snapPageStats[s];

      if (pageStats >= spaceNeeded) {
        // CHECK IF THE SPACE AVAILABLE IS REAL
        final PageAnalysis pageAnalysis = getAvailableSpaceInPage(pageId, spaceNeeded, multiPageRecord);

        if (pageAnalysis.totalRecordsInPage >= maxRecordsInPage) {
          if (pagesToRemove == null)
            pagesToRemove = new int[snapSize];
          pagesToRemove[pagesToRemoveCount++] = pageId;
          continue;
        }

        if (!pageAnalysis.createNewPage) {
          if (pageAnalysis.spaceAvailableInCurrentPage != pageStats) {
            // LOW COST UPDATE OF STATISTICS
            if (pageAnalysis.spaceAvailableInCurrentPage < MINIMUM_SPACE_LEFT_IN_PAGE) {
              if (pagesToRemove == null)
                pagesToRemove = new int[snapSize];
              pagesToRemove[pagesToRemoveCount++] = pageId;
            } else
              freeSpaceInPages.put(pageId, pageAnalysis.spaceAvailableInCurrentPage);
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
      for (int i = 0; i < pagesToRemoveCount; i++)
        freeSpaceInPages.remove(pagesToRemove[i], -1);

    return bestPageAnalysis;
  }

  /**
   * Gather statistics about the free space in pages. Algorithm:
   * 1. browse all the pages but the latest, and add in a tree map the pages with enough free space (>GATHER_STATS_MIN_SPACE_PERC)
   * 2. save in the map recordCountInPage and freeSpaceInPage
   * 3. if the tree map is full (size > MAX_PAGES_GATHER_STATS), stop
   */
  public void gatherPageStatistics() {
    final boolean firstRun = timeOfLastStats == 0L;
    if (!firstRun && System.currentTimeMillis() - timeOfLastStats <= MAX_TIMEOUT_GATHER_STATS)
      return;

    // #5063: consume the change counter atomically at the decision point. The previous
    // get() > 0 check paired with a set(0L) at the end of the scan wiped any increment landing while the
    // scan ran; getAndSet(0L) carries those increments into the next cycle instead of losing them.
    final long consumedChanges = changesFromLastStats.getAndSet(0L);
    if (consumedChanges > 0 || firstRun)
      try {
        int txPageCount = getTotalPages();

        synchronized (freeSpaceInPages) {
          for (int pageId = 0; pageId < txPageCount - 2; ++pageId) {
            final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
            final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
            final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage);

            // #4958: measure against the usable content region (getMaxContentSize), not the physical
            // page size: the latter overstated the free space of every page by the page header size.
            final int freeSpaceInPage = freeTailInPage(page, orderedRecordContentInPage);

            final int freeSpacePerc = freeSpaceInPage * 100 / (page.getMaxContentSize() - contentHeaderSize);

            if (freeSpacePerc > GATHER_STATS_MIN_SPACE_PERC)
              freeSpaceInPages.put(pageId, freeSpaceInPage);

            if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS)
              break;
          }

          timeOfLastStats = System.currentTimeMillis();
        }
      } catch (Exception e) {
        // #5063: THE COUNTER WAS ALREADY CONSUMED. RESTORE THE FULL CONSUMED COUNT (NOT A
        // SINGLE INCREMENT, WHICH UNDERCOUNTED THE PENDING CHANGES) SO THE FAILED SCAN IS RETRIED AT THE
        // NEXT CYCLE; max(consumed, 1) COVERS THE firstRun CASE WHERE THE CONSUMED COUNT MAY BE ZERO
        changesFromLastStats.addAndGet(Math.max(consumedChanges, 1L));
        LogManager.instance().log(this, Level.WARNING, "Error on gathering statistics on bucket '%s'", e, getName());
      }
  }

  /**
   * Update the in memory statistics about the free space in page.
   * <p>
   * The two arguments describe ONE quantity between them: {@code availableSpace + delta} is the free space the page
   * has once the caller's write has landed, and that is the only thing recorded. Both halves are the caller's own
   * measurement - there is no "I do not know" to pass.
   * <p>
   * #6358: there used to be one, undeclared and indistinguishable from a real answer. An {@code availableSpace} of 0
   * was read as "apply my delta to whatever you already hold" whenever an entry existed for the page, so a caller
   * reporting a page with genuinely no free tail left had its delta added to a stale number instead of to 0. Nothing
   * in the signature said so, and no caller ever wanted it: every one of them measures the page it is writing to.
   * The overload is gone rather than given a sentinel of its own, because a sentinel nobody passes is a second
   * meaning waiting for the next caller to find by accident.
   *
   * @param page           the page image this write landed on. Takes the page rather than its number so the two
   *                       cannot disagree, and so the claim of #6396 can be kept on the very object the commit
   *                       measures.
   * @param availableSpace free space the page had before this write, measured against
   *                       {@link BasePage#getMaxContentSize()} - never {@code getAvailableContentSize()}, which
   *                       still counts the page header.
   * @param delta          change this write makes to it: negative for space taken, positive for space given back,
   *                       and 0 when {@code availableSpace} is a measurement of the settled page rather than a
   *                       change to it (see {@link #accountCompressedPage}).
   */
  private void updatePageStatistics(final MutablePage page, final int availableSpace, final int delta) {
    changesFromLastStats.incrementAndGet();

    if (reuseSpaceMode.ordinal() < REUSE_SPACE_MODE.HIGH.ordinal())
      return;

    final int pageId = page.getPageId().getPageNumber();

    // A page cannot have less than no free space. Under -ea (surefire's default) this is the tripwire that says a
    // write path and the commit-time measurement have stopped describing the same bytes; in production the branches
    // below absorb it as they always did.
    assert availableSpace >= 0 && availableSpace + delta >= 0 :
        "page " + pageId + " of bucket '" + componentName + "' is reported with " + availableSpace + " free bytes and a delta of "
            + delta;

    // #6396: what this write says the page's free tail is, kept on the page image itself so the commit can hold it
    // against the measurement it makes on the very same object. Deliberately NOT the entry in freeSpaceInPages: that
    // map is bucket-wide (a concurrent transaction's write to the same page would be compared against ours), it is
    // thresholded and evicted (most pages carry no entry at all), and it is also written from outside by
    // setPageStatistics and gatherPageStatistics, neither of which is a claim any writer made.
    if (CHECK_FREE_SPACE_CLAIMS)
      page.claimFreeSpace(availableSpace + delta);

    synchronized (freeSpaceInPages) {
      if (availableSpace + delta == 0)
        freeSpaceInPages.remove(pageId, -1);
      else {
        // #5067: same usable-space base as gatherPageStatistics() (#4958): measure against the usable
        // content region (physical page size minus the page header), not the physical page size, which
        // overstated the space of every page and skewed the GATHER_STATS_MIN_SPACE_PERC threshold
        final int usableSpaceInPage = getPageSize() - BasePage.PAGE_HEADER_SIZE - contentHeaderSize;

        final boolean hasEntry = freeSpaceInPages.containsKey(pageId);
        final int newSpace = availableSpace + delta;

        if (hasEntry) {
          if (newSpace <= MINIMUM_SPACE_LEFT_IN_PAGE || (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS
                  && newSpace * 100 / usableSpaceInPage < GATHER_STATS_MIN_SPACE_PERC))
            freeSpaceInPages.remove(pageId, -1);
          else
            freeSpaceInPages.put(pageId, newSpace);
        } else if (newSpace * 100 / usableSpaceInPage >= GATHER_STATS_MIN_SPACE_PERC) {
          if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS) {
            // REMOVE THE SMALLEST PAGE
            final int[] lowestPageId = { -1 };
            final int[] lowestPageSpace = { -1 };

            freeSpaceInPages.forEach((k, v) -> {
              if (lowestPageId[0] < 0 || v < lowestPageSpace[0]) {
                lowestPageId[0] = k;
                lowestPageSpace[0] = v;
              }
            });

            if (lowestPageId[0] > -1 && lowestPageSpace[0] < newSpace) {
              freeSpaceInPages.remove(lowestPageId[0], -1);
              freeSpaceInPages.put(pageId, newSpace);
            }
          } else
            freeSpaceInPages.put(pageId, newSpace);
        }
      }
    }
  }

  /**
   * The free-space HINT currently held for a page, or -1 when there is none: the value
   * {@link #findAvailableSpaceFromStatistics} screens candidate pages with. Package-private and read-only, it exists
   * so what the write paths feed into the statistics can be asserted for what it is (#6154) - the allocator re-reads
   * every candidate page before using it, so a test driven through the allocator alone would pass whether or not the
   * statistics were ever told.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  int getFreeSpaceHintForPage(final int pageId) {
    synchronized (freeSpaceInPages) {
      return freeSpaceInPages.get(pageId, -1);
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

  /**
   * Insertion sort on two parallel arrays, ordering by ascending {@code pageIds[i]}. Bounded to
   * MAX_PAGES_GATHER_STATS (100) entries so insertion sort is the right choice (low constant,
   * great cache behavior, in-place).
   */
  private static void sortByPageIdAscending(final int[] pageIds, final int[] pageStats, final int n) {
    for (int i = 1; i < n; i++) {
      final int kPage = pageIds[i];
      final int kStats = pageStats[i];
      int j = i - 1;
      while (j >= 0 && pageIds[j] > kPage) {
        pageIds[j + 1] = pageIds[j];
        pageStats[j + 1] = pageStats[j];
        j--;
      }
      pageIds[j + 1] = kPage;
      pageStats[j + 1] = kStats;
    }
  }

  public JSONObject getStatistics() {
    final JSONObject json = new JSONObject();

    final long cachedCount = getCachedRecordCount();
    if (cachedCount > -1)
      json.put("count", cachedCount);

    final JSONArray pages = new JSONArray();
    synchronized (freeSpaceInPages) {
      freeSpaceInPages.forEach((k, v) -> pages.put(new JSONObject().put("id", k).put("free", v)));
    }
    json.put("pages", pages);

    return json;
  }
}
