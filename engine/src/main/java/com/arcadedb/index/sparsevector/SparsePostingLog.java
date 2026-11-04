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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;

import java.io.IOException;

/**
 * Append-only log of the postings a sparse vector index committed and has not sealed into a segment yet (issue #11239).
 * <p>
 * The memtable of {@link PaginatedSparseVectorEngine} lives in RAM until it is sealed, past a million postings or at a clean
 * close, so a process that died in between came back with every record (the WAL replays them) and none of their postings. Each
 * commit now also appends its postings to one of these pages, in the very transaction that writes the records, so the page WAL
 * makes them durable together and a transaction that rolls back takes its entries back with it.
 * <p>
 * <b>The log is what the memtable is made of (issue #11257).</b> The engine never publishes a logged posting into its memtable by
 * itself: it decodes the committed pages of its logs, past a cursor each log keeps, at the end of each commit that appended, at
 * the first use after an open (the replay), and on a replicated database after the pages of an entry are applied, whichever node
 * wrote it. The leader and every follower therefore hold the same unsealed postings, in the order the log committed them, and a
 * follower promoted to leader already holds what the previous one had not sealed.
 * <p>
 * An engine owns two of them and writes to one at a time. A flush first CLOSES the log the commits append to, in a transaction of
 * its own, so they move to the other one; it then seals the memtable decoded from the closed log and resets that log once the
 * segment is on disk. Every append rewrites the state byte of the header too, so a commit prepared against a log that a close or
 * a reset got to first fails its page version check and is retried, on the other log: on a replicated database that is what
 * keeps a commit originating on a follower from appending to a log the leader is sealing. When both logs hold postings (a reset
 * that did not commit) there is no empty log to move to: the flush seals what it holds, resets the other log only, and the commits
 * keep appending to the same one behind postings already sealed, which a later flush resets. Replaying such a log re-applies
 * sealed postings in their commit order, which changes nothing.
 * <p>
 * <b>Layout.</b> Page 0 is the header: a magic number, the generation of the log's content and its state (empty, active, or
 * closed: holding postings and taking no more). Every following page starts with the generation that wrote it and holds entries
 * up to its content size: dimension, bucket id and position of the RID as variable-length numbers, then the weight as a 4-byte
 * float, NaN for a tombstone. An entry never spans two pages. A reset only marks the header empty, and a new generation reuses
 * the pages from page 1 on: the generation stamped on each page is what tells a page of the current generation from a stale one,
 * and generations only grow, because the header keeps its generation when it is reset.
 * <p>
 * <b>Concurrency.</b> Both logs belong to the index's file set ({@link LSMSparseVectorIndex#getFileIds()}), so every commit that
 * appends holds both files' commit locks from before its first append until after its postings are decoded into the memtable.
 * The appender finds the tail it appends to in the committed pages, never in what this node remembers, because on a replicated
 * database the other nodes append too. The decode cursor and the committed header mirrored here are written under the engine's
 * memtable lock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SparsePostingLog extends PaginatedComponent {
  public static final String FILE_EXT        = "sparselog";
  public static final int    CURRENT_VERSION = 0;

  private static final int  HEADER_PAGE        = 0;
  private static final int  MAGIC              = 0x53504c47; // "SPLG"
  private static final int  HEADER_MAGIC       = 0;
  private static final int  HEADER_GENERATION  = HEADER_MAGIC + Binary.INT_SERIALIZED_SIZE;
  private static final int  HEADER_STATE       = HEADER_GENERATION + Binary.LONG_SERIALIZED_SIZE;
  private static final byte STATE_EMPTY        = 0;
  private static final byte STATE_ACTIVE       = 1;
  private static final byte STATE_CLOSED       = 2;
  private static final int  PAGE_GENERATION    = 0;
  private static final int  FIRST_ENTRY_OFFSET = Binary.LONG_SERIALIZED_SIZE;
  /** Largest entry: 3 variable-length numbers (an int, an int, a long) and the 4-byte weight. */
  private static final int  MAX_ENTRY_SIZE     = 5 + 5 + 10 + Binary.FLOAT_SERIALIZED_SIZE;

  private volatile PaginatedSparseVectorEngine engine;

  // The committed header, as the last read of it found it.
  private volatile long generation;
  private volatile byte state = STATE_EMPTY;

  // The decode cursor: the generation whose entries the engine's memtables hold, 0 for none, and the position right after the
  // last entry decoded. Also where the next appender starts looking for the tail, which it verifies against the pages.
  private volatile long decodedGeneration;
  private volatile int  decodedPage;
  private          int  decodedOffset;

  /** Constructor used at creation time (a fresh component file). */
  protected SparsePostingLog(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
  }

  /** Constructor used at load time (an existing file with a known file id). */
  protected SparsePostingLog(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  /**
   * {@link ComponentFactory} handler so the {@code .sparselog} files registered with the database are reopened as
   * {@code SparsePostingLog} instances at startup, where their engine finds them by name.
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new SparsePostingLog(database, name, filePath, id, mode, pageSize, version);
    }
  }

  @Override
  public Object getMainComponent() {
    return this;
  }

  /** Sets the engine whose memtables this log feeds. */
  void attach(final PaginatedSparseVectorEngine engine) {
    this.engine = engine;
  }

  /**
   * Called once the pages of a replicated entry, or of a replayed one, are applied to this log, whichever node wrote it (issue
   * #11257): the engine decodes what they added into its memtables.
   */
  public void onPagesApplied() {
    final PaginatedSparseVectorEngine owner = engine;
    if (owner != null)
      owner.catchUpWithPostingLogs();
  }

  public long getGeneration() {
    return generation;
  }

  /** Whether the log holds postings of a generation that was not reset yet: active, or closed by a flush that seals them. */
  public boolean isActive() {
    return state != STATE_EMPTY;
  }

  /** Whether the log is active and takes appends. */
  boolean isAppendable() {
    return state == STATE_ACTIVE;
  }

  /**
   * Reads the committed header. Called at the first use after an open, never by the open itself: the schema load that builds the
   * engine runs before the WAL recovery restores the pages a crash left only in the WAL. A file that has no page yet, or whose
   * header is not one of ours, is an empty log.
   */
  void readCommittedHeader() throws IOException {
    if (getTotalPages() == 0) {
      state = STATE_EMPTY;
      return;
    }
    final BasePage header = readCommittedPage(HEADER_PAGE);
    if (header.readInt(HEADER_MAGIC) != MAGIC) {
      state = STATE_EMPTY;
      return;
    }
    generation = header.readLong(HEADER_GENERATION);
    state = header.readByte(HEADER_STATE);
  }

  /** The generation whose entries were decoded, 0 for none. */
  long getDecodedGeneration() {
    return decodedGeneration;
  }

  /** Moves the decode cursor to the start of {@code newGeneration}, or nowhere for 0. */
  void startDecoding(final long newGeneration) {
    decodedGeneration = newGeneration;
    decodedPage = 1;
    decodedOffset = FIRST_ENTRY_OFFSET;
  }

  /**
   * Decodes the committed entries past the cursor into {@code target}, in the order they were committed, and moves the cursor
   * after them. Nothing when no generation is being decoded.
   *
   * @return the entries decoded
   */
  long decodeInto(final Memtable target) throws IOException {
    if (decodedGeneration == 0L)
      return 0L;
    long decoded = 0L;
    final int totalPages = getTotalPages();
    while (decodedPage < totalPages) {
      final BasePage page = readCommittedPage(decodedPage);
      if (page.readLong(PAGE_GENERATION) != decodedGeneration)
        break;
      final int end = page.getContentSize();
      int offset = decodedOffset;
      while (offset < end) {
        final int dim = (int) page.readNumber(offset);
        offset += page.readNumberSize(offset);
        final int bucketId = (int) page.readNumber(offset);
        offset += page.readNumberSize(offset);
        final long position = page.readNumber(offset);
        offset += page.readNumberSize(offset);
        final float weight = page.readFloat(offset);
        offset += Binary.FLOAT_SERIALIZED_SIZE;

        final RID rid = new RID(bucketId, position);
        if (Float.isNaN(weight))
          target.remove(dim, rid);
        else
          target.put(dim, rid, weight);
        ++decoded;
      }
      decodedOffset = Math.max(decodedOffset, end);
      // The appender moves to the next page only once this one has no room for an entry: until then nothing follows it
      if (end + MAX_ENTRY_SIZE <= page.getMaxContentSize())
        break;
      if (decodedPage + 1 >= totalPages || readCommittedPage(decodedPage + 1).readLong(PAGE_GENERATION) != decodedGeneration)
        break;
      ++decodedPage;
      decodedOffset = FIRST_ENTRY_OFFSET;
    }
    return decoded;
  }

  /** Marks the log empty inside {@code tx}. The pages stay allocated and the next generation overwrites them from page 1 on. */
  void reset(final TransactionContext tx) throws IOException {
    if (getTotalPages() == 0)
      return;
    final MutablePage header = tx.getPageToModify(new PageId(database, fileId, HEADER_PAGE), pageSize, false);
    header.writeByte(HEADER_STATE, STATE_EMPTY);
  }

  /**
   * Closes the active log to appends inside {@code tx}, keeping its postings: what a flush does before it seals them, so the
   * commits move to the other log. A commit that appended to it concurrently conflicts on the header (see {@link Appender}).
   */
  void closeToAppends(final TransactionContext tx) throws IOException {
    if (getTotalPages() == 0)
      return;
    final MutablePage header = tx.getPageToModify(new PageId(database, fileId, HEADER_PAGE), pageSize, false);
    if (header.readInt(HEADER_MAGIC) == MAGIC && header.readByte(HEADER_STATE) == STATE_ACTIVE)
      header.writeByte(HEADER_STATE, STATE_CLOSED);
  }

  /**
   * Starts appending the postings of {@code tx}, or answers null when the committed header says the log is closed: the caller
   * then appends to the other log. {@code newGeneration} is stamped when the log is empty, which makes it the newest log, and
   * ignored otherwise. Called with this log's commit lock held.
   */
  Appender openAppender(final TransactionContext tx, final long newGeneration) {
    try {
      final MutablePage header = pageToWrite(tx, HEADER_PAGE);
      final boolean ours = header.readInt(HEADER_MAGIC) == MAGIC;
      final byte committedState = ours ? header.readByte(HEADER_STATE) : STATE_EMPTY;
      if (committedState == STATE_CLOSED)
        return null;
      return new Appender(tx, header, committedState == STATE_ACTIVE,
          committedState == STATE_ACTIVE ? header.readLong(HEADER_GENERATION) :
              Math.max(newGeneration, (ours ? header.readLong(HEADER_GENERATION) : 0L) + 1L));
    } catch (final IOException e) {
      throw new IndexException("Cannot append to the sparse posting log '" + getName() + "'", e);
    }
  }

  private BasePage readCommittedPage(final int pageNumber) throws IOException {
    return database.getPageManager().getImmutablePage(new PageId(database, fileId, pageNumber), pageSize, false, false);
  }

  private MutablePage pageToWrite(final TransactionContext tx, final int pageNumber) throws IOException {
    final PageId pageId = new PageId(database, fileId, pageNumber);
    if (pageNumber < getTotalPages())
      return tx.getPageToModify(pageId, pageSize, false);
    return tx.addPage(pageId, pageSize);
  }

  /** The entries one transaction appends, on its own pages until it commits. */
  final class Appender {
    private final TransactionContext tx;
    private final long               appenderGeneration;
    private       MutablePage        page;
    private       int                pageNumber;
    private       int                offset;

    private Appender(final TransactionContext tx, final MutablePage header, final boolean active, final long appenderGeneration)
        throws IOException {
      this.tx = tx;
      this.appenderGeneration = appenderGeneration;
      if (active) {
        // Rewritten although unchanged: the header is then among the pages this commit modifies, and its version check fails
        // when a close or a reset committed after this commit read it
        header.writeByte(HEADER_STATE, STATE_ACTIVE);
        pageNumber = findTail();
        page = pageToWrite(tx, pageNumber);
        offset = page.getContentSize();
      } else {
        header.writeInt(HEADER_MAGIC, MAGIC);
        header.writeLong(HEADER_GENERATION, appenderGeneration);
        header.writeByte(HEADER_STATE, STATE_ACTIVE);
        startPage(1);
      }
    }

    void append(final int dim, final RID rid, final float weight) {
      try {
        if (offset + MAX_ENTRY_SIZE > page.getMaxContentSize())
          startPage(pageNumber + 1);
        offset += page.writeNumber(offset, dim);
        offset += page.writeNumber(offset, rid.getBucketId());
        offset += page.writeNumber(offset, rid.getPosition());
        page.writeFloat(offset, weight);
        offset += Binary.FLOAT_SERIALIZED_SIZE;
        page.setContentSize(offset);
      } catch (final IOException e) {
        throw new IndexException("Cannot append to the sparse posting log '" + getName() + "'", e);
      }
    }

    /**
     * The last committed page of the generation. Searched from the decode cursor when it is on this generation, which is where
     * the tail almost always is; the committed pages decide, since another node may have appended since.
     */
    private int findTail() throws IOException {
      int tail = decodedGeneration == appenderGeneration ? decodedPage : 1;
      if (tail >= getTotalPages() || readCommittedPage(tail).readLong(PAGE_GENERATION) != appenderGeneration)
        tail = 1;
      while (tail + 1 < getTotalPages()) {
        final BasePage current = readCommittedPage(tail);
        // A page with room for an entry is the last one: appends move to the next page only once it is full
        if (current.getContentSize() + MAX_ENTRY_SIZE <= current.getMaxContentSize()
            || readCommittedPage(tail + 1).readLong(PAGE_GENERATION) != appenderGeneration)
          break;
        ++tail;
      }
      return tail;
    }

    private void startPage(final int number) throws IOException {
      pageNumber = number;
      page = pageToWrite(tx, number);
      page.writeLong(PAGE_GENERATION, appenderGeneration);
      offset = FIRST_ENTRY_OFFSET;
      page.setContentSize(offset);
    }
  }
}
