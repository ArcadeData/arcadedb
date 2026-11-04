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
 * makes them durable together and a transaction that rolls back takes its entries back with it. At the next open the engine
 * replays the log into the memtable.
 * <p>
 * An engine owns two of them and writes to one at a time. A flush swaps the memtable and the log together, so the memtable it
 * seals holds exactly the entries of the log it leaves, and resets that log once the segment is on disk. When both logs hold
 * postings (after a crash replayed both, or a reset that did not commit) there is no empty log to move to: the flush seals
 * everything, resets the older log only, and the commits keep appending to the newer one behind postings already sealed, which
 * the next flush resets. Replaying such a log re-applies sealed postings in their commit order, which changes nothing.
 * <p>
 * <b>Layout.</b> Page 0 is the header: a magic number, the generation of the log's content and its state (active or empty).
 * Every following page starts with the generation that wrote it and holds entries up to its content size: dimension, bucket id
 * and position of the RID as variable-length numbers, then the weight as a 4-byte float, NaN for a tombstone. An entry never
 * spans two pages. A reset only marks the header empty, and a new generation reuses the pages from page 1 on: the generation
 * stamped on each page is what tells a page of the current generation from a stale one, and generations only grow, because the
 * header keeps its generation when it is reset.
 * <p>
 * <b>Concurrency.</b> Both logs belong to the index's file set ({@link LSMSparseVectorIndex#getFileIds()}), so every commit that
 * appends holds both files' commit locks from before its first append until after it published its postings into the memtable.
 * The engine takes the same two locks to swap the logs, to replay them and to reset one, which is what keeps a log and its
 * memtable aligned. A reset never touches the log the commits append to.
 * The committed tail of a log is mirrored in memory and moved only by a commit that published, under those locks.
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
  private static final int  PAGE_GENERATION    = 0;
  private static final int  FIRST_ENTRY_OFFSET = Binary.LONG_SERIALIZED_SIZE;
  /** Largest entry: 3 variable-length numbers (an int, an int, a long) and the 4-byte weight. */
  private static final int  MAX_ENTRY_SIZE     = 5 + 5 + 10 + Binary.FLOAT_SERIALIZED_SIZE;

  // The committed state, mirrored from the pages. Written by a commit that published and by a reset, both under this log's
  // commit lock; read under it by the next appender.
  private volatile long    generation;
  private volatile boolean active;
  private volatile int     tailPage;

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

  public long getGeneration() {
    return generation;
  }

  /** Whether the log holds postings of a generation that was not reset yet. */
  public boolean isActive() {
    return active;
  }

  /**
   * Reads the committed header and finds the tail. Called at the first use after an open, never by the open itself: the schema
   * load that builds the engine runs before the WAL recovery restores the pages a crash left only in the WAL. A file that has no
   * page yet, or whose header is not one of ours, is an empty log.
   */
  void loadState() throws IOException {
    generation = 0L;
    active = false;
    tailPage = 0;
    if (getTotalPages() == 0)
      return;

    final BasePage header = readCommittedPage(HEADER_PAGE);
    if (header.readInt(HEADER_MAGIC) != MAGIC)
      return;
    generation = header.readLong(HEADER_GENERATION);
    active = header.readByte(HEADER_STATE) == STATE_ACTIVE;
    if (!active)
      return;

    int last = 0;
    for (int p = 1; p < getTotalPages(); p++) {
      if (readCommittedPage(p).readLong(PAGE_GENERATION) != generation)
        break;
      last = p;
    }
    tailPage = last;
    if (last == 0)
      // a header that announced a generation no page carries: nothing was committed into it
      active = false;
  }

  /**
   * Replays the committed entries into {@code target}, in the order they were committed. Called with both logs' commit locks
   * held, so no commit is half way between its append and its publication.
   *
   * @return the entries replayed
   */
  long replayInto(final Memtable target) throws IOException {
    if (!active)
      return 0L;
    long replayed = 0L;
    for (int p = 1; p <= tailPage; p++) {
      final BasePage page = readCommittedPage(p);
      if (page.readLong(PAGE_GENERATION) != generation)
        break;
      final int end = page.getContentSize();
      int offset = FIRST_ENTRY_OFFSET;
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
        ++replayed;
      }
    }
    return replayed;
  }

  /**
   * Marks the log empty inside {@code tx}. The pages stay allocated and the next generation overwrites them from page 1 on.
   */
  void reset(final TransactionContext tx) throws IOException {
    if (getTotalPages() == 0)
      return;
    final MutablePage header = tx.getPageToModify(new PageId(database, fileId, HEADER_PAGE), pageSize, false);
    header.writeByte(HEADER_STATE, STATE_EMPTY);
  }

  /** The committed state after a reset that committed. */
  void resetCommitted() {
    active = false;
    tailPage = 0;
  }

  /**
   * Starts appending the postings of {@code tx}. {@code newGeneration} is stamped when the log is empty, which makes it the
   * newest log, and ignored otherwise. Called with this log's commit lock held.
   */
  Appender openAppender(final TransactionContext tx, final long newGeneration) {
    return new Appender(tx, newGeneration);
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

    private Appender(final TransactionContext tx, final long newGeneration) {
      this.tx = tx;
      try {
        if (active) {
          appenderGeneration = generation;
          pageNumber = tailPage;
          page = pageToWrite(tx, pageNumber);
          offset = page.getContentSize();
        } else {
          appenderGeneration = newGeneration;
          final MutablePage header = pageToWrite(tx, HEADER_PAGE);
          header.writeInt(HEADER_MAGIC, MAGIC);
          header.writeLong(HEADER_GENERATION, appenderGeneration);
          header.writeByte(HEADER_STATE, STATE_ACTIVE);
          startPage(1);
        }
      } catch (final IOException e) {
        throw new IndexException("Cannot append to the sparse posting log '" + getName() + "'", e);
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

    /** The transaction committed: its tail is the log's committed tail. Called before its commit locks are released. */
    void committed() {
      generation = appenderGeneration;
      tailPage = pageNumber;
      active = true;
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
