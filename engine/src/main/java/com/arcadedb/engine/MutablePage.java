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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.TrackableBinary;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Mutable page that accepts updates. It keeps track of the modified bytes.
 * <br>
 * NOTE: This class is not thread safe and must be not used by multiple threads at the same time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MutablePage extends BasePage implements TrackableContent {
  /**
   * Commit-time page-replay mechanism: the commutative edge-append merge
   * ({@code TransactionContext.rebaseEdgeAppends}).
   */
  public static final  int     COVERAGE_EDGE_APPEND_MERGE = 1;
  /**
   * Commit-time page-replay mechanism: the disjoint-slot merge ({@code TransactionContext.rebaseSlots}).
   */
  public static final  int     COVERAGE_SLOT_MERGE        = 1 << 1;
  /**
   * Every commit-time page-replay mechanism at once: used by writers whose changes are re-applied by ALL of them
   * (e.g. {@code LocalBucket.compressPage}, which the commit path re-runs on the rebased page).
   */
  public static final  int     COVERAGE_ALL_MERGES        = COVERAGE_EDGE_APPEND_MERGE | COVERAGE_SLOT_MERGE;
  /**
   * Maximum number of disjoint modified intervals tracked per page. Page layouts write at both ends (an LSM index
   * page grows its sorted pointer array up from the header and its key/value content down from the tail; a bucket
   * page does the same with its record pointer table), so two or three intervals cover the real cases. The extra
   * slots absorb the occasional in-place update far from either end; past this budget the two closest intervals are
   * merged, which only makes the tracking coarser (never wrong) and degrades at worst to the single-hull behavior
   * this class had before issue #5470.
   */
  private static final int     MAX_MODIFIED_RANGES        = 8;
  private static final byte[]  ZERO_BYTES_ARRAY;
  // Sorted, disjoint and non-adjacent intervals as [from0,to0, from1,to1, ...]. One extra slot is kept free so an
  // insertion can happen before the merge that puts the count back within budget.
  private final        int[]   modifiedRanges             = new int[(MAX_MODIFIED_RANGES + 1) * 2];
  private              int     modifiedRangeCount         = 0;
  // Replay-coverage bookkeeping (issue #5596). declaredCoverage is the set of mechanisms that own the write
  // currently in progress; uncoveredMechanisms is the sticky set of mechanisms that CANNOT account for at least one
  // byte this transaction dirtied on this page. See beginCoveredWrite/isFullyCoveredBy.
  // Plain ints on purpose, like every other field of this class: a MutablePage is one transaction's private image of
  // a page (see the class note above), dirtied only by the thread that owns that transaction and read on the
  // committing thread - which is the same thread, or one the commit hand-off already orders against it. Do NOT
  // introduce a background mutator of a modified image: it would need synchronisation here, and an unsynchronised
  // one could hide a dirtied byte from the coverage proof, which is the very hazard #5596 exists to remove.
  private              int     declaredCoverage           = 0;
  private              int     uncoveredMechanisms        = 0;
  // AtomicReference so the WAL ack can be taken EXACTLY ONCE (#4928 review): the success path, the
  // file-dropped flush branch and the dropped-file batch purge can race on the same page (the flush loop
  // does not remove pages from the batch list), and a double notifyPageFlushed would steal another page's
  // pending ack - letting the close-time ack gate delete a WAL that still holds unflushed committed data.
  private final        java.util.concurrent.atomic.AtomicReference<WALFile> walFile = new java.util.concurrent.atomic.AtomicReference<>();

  static {
    ZERO_BYTES_ARRAY = new byte[GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger()];
  }

  public MutablePage(final PageId pageId, final int size) {
    this(pageId, size, new byte[size], 0, 0);
    updateModifiedRange(0, size - 1);
  }

  public MutablePage(final PageId pageId, final int size, final byte[] array, final int version, final int contentSize) {
    super(pageId, size, array, version, contentSize);
  }

  /**
   * Returns a copy of the underlying buffer because it could change in the current thread.
   */
  @Override
  public Binary getImmutableView(final int index, final int length) {
    final int offset = content.getByteBuffer().arrayOffset() + index + PAGE_HEADER_SIZE;
    final Binary copy = new Binary(Arrays.copyOfRange(content.getContent(), offset, offset + length));
    copy.setAutoResizable(true);
    return copy;
  }

  @Override
  public MutablePage modify() {
    return this;
  }

  public TrackableBinary getTrackable() {
    final ByteBuffer buffer = content.getByteBuffer();
    buffer.position(PAGE_HEADER_SIZE);
    return new TrackableBinary(this, buffer.slice());
  }

  public void setContentSize(final int value) {
    content.size(value + PAGE_HEADER_SIZE);
  }

  public void clearContent() {
    content.clear();
  }

  public void updateMetadata() {
    content.putInt(PAGE_VERSION_OFFSET, version);
    content.putInt(PAGE_CONTENTSIZE_OFFSET, content.size());
  }

  public void incrementVersion() {
    version++;
  }

  public int writeNumber(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    final int serializedSize = Binary.getNumberSpace(content); // PRE-CALCULATE THE SIZE
    checkBoundariesOnWrite(index, serializedSize);
    return this.content.putNumber(index, content);
  }

  public int writeLong(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.LONG_SERIALIZED_SIZE);
    this.content.putLong(index, content);
    return Binary.LONG_SERIALIZED_SIZE;
  }

  public int writeInt(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, content);
    return Binary.INT_SERIALIZED_SIZE;
  }

  public int writeUnsignedInt(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, (int) content);
    return Binary.INT_SERIALIZED_SIZE;
  }

  public void writeShort(int index, final short content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, content);
  }

  public void writeUnsignedShort(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, (short) content);
  }

  public void writeFloat(int index, final float content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.FLOAT_SERIALIZED_SIZE);
    this.content.putFloat(index, content);
  }

  public void writeDouble(int index, final double content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.DOUBLE_SERIALIZED_SIZE);
    this.content.putDouble(index, content);
  }

  public int writeByte(int index, final byte content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.BYTE_SERIALIZED_SIZE);
    this.content.putByte(index, content);
    return Binary.BYTE_SERIALIZED_SIZE;
  }

  public int writeBytes(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    final int varSizeBytesUsed = Binary.getUnsignedNumberSpace(content.length);
    checkBoundariesOnWrite(index, varSizeBytesUsed + content.length);
    return this.content.putBytes(index, content);
  }

  public void writeByteArray(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, content.length);
    this.content.putByteArray(index, content);
  }

  public void writeByteArray(int index, final byte[] content, final int contentOffset, final int contentSize) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, contentSize);
    this.content.putByteArray(index, content, contentOffset, contentSize);
  }

  public void writeZeros(int index, final int contentLength) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, contentLength);
    if (contentLength <= ZERO_BYTES_ARRAY.length)
      // FAST COPY
      this.content.putByteArray(index, ZERO_BYTES_ARRAY, 0, contentLength);
    else
      for (int i = 0; i < contentLength; i++)
        this.content.putByte(index + i, (byte) 0);
  }

  public int writeString(final int index, final String content) {
    return writeBytes(index, content.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  /**
   * Returns the smallest interval that contains every modified byte, i.e. the hull of {@link #getModifiedRanges()}.
   * Callers that must ship the changes (the WAL) use the individual intervals instead, because the hull of a page
   * written at both ends covers the whole page (issue #5470).
   */
  @Override
  public int[] getModifiedRange() {
    if (modifiedRangeCount == 0)
      return new int[] { Integer.MAX_VALUE, -1 };
    return new int[] { modifiedRanges[0], modifiedRanges[modifiedRangeCount * 2 - 1] };
  }

  /**
   * Returns the disjoint modified intervals as [from0,to0, from1,to1, ...] in ascending order, valid up to
   * {@code 2 * }{@link #getModifiedRangeCount()}. The array is the live internal one: read it, never keep it.
   */
  public int[] getModifiedRanges() {
    return modifiedRanges;
  }

  public int getModifiedRangeCount() {
    return modifiedRangeCount;
  }

  /**
   * Declares that every byte the caller is about to write on this page can be re-derived at commit time by the given
   * commit-time replay mechanism(s) - a bit set of {@link #COVERAGE_EDGE_APPEND_MERGE} and
   * {@link #COVERAGE_SLOT_MERGE} - and returns the previous declaration so it can be restored by
   * {@link #endCoveredWrite(int)}. Nesting is therefore supported; callers MUST restore in a {@code finally}.
   * <p>
   * This is the opt-IN half of issue #5596: a commit-time page merge drops this transaction's whole image of the
   * conflicting page and replays only its tracked writes on top of the newer committed version, which is sound only
   * if every byte the transaction wrote is accounted for by one of those tracked writes. Instead of trusting each
   * writer to opt OUT (the {@code poisonEdgeAppendPage}/{@code poisonSlotRebasePage} calls, one forgotten call = a
   * silently lost write), a byte counts as covered only while its writer says so: any write performed outside a
   * declaration permanently marks the page as not re-derivable by the mechanisms that did not declare it.
   * <p>
   * NOTE: this REPLACES the current declaration, it does not union with it - a {@code begin(EDGE)} nested inside a
   * {@code begin(SLOT)} scope declares its writes EDGE-only, not both. That is deliberate: coverage must name the
   * mechanism that actually replays each byte, and a union would let an outer scope vouch for writes it cannot
   * reproduce. Pass the full mask when a write really is replayed by several mechanisms (as
   * {@code compressPage} does with {@link #COVERAGE_ALL_MERGES}).
   */
  public int beginCoveredWrite(final int mechanisms) {
    final int previous = declaredCoverage;
    // The one thing that could turn this backstop into the very hazard it removes: a writer that opens a declaration
    // and never restores it (a missing finally, an exception unwinding past it) leaves the declaration live, so a
    // LATER undeclared write to the same page is wrongly vouched for. No writer legitimately opens a non-zero
    // declaration inside another one - each names the single mechanism that replays its own bytes, and a write
    // replayed by several of them passes the full mask instead - so inheriting a live declaration means exactly that
    // leak. An assertion, not a hard check: it fails fast under -ea (surefire's default) at zero production cost,
    // where the sticky uncovered set still keeps the outcome safe.
    assert previous == 0 || mechanisms == 0 :
        "A covered-write declaration leaked on page " + pageId + " (declared=" + previous
            + "): every beginCoveredWrite must be restored by endCoveredWrite in a finally block";
    declaredCoverage = mechanisms;
    return previous;
  }

  /**
   * Restores the declaration {@link #beginCoveredWrite(int)} replaced. Always call it from a {@code finally} block:
   * leaving a stale declaration behind would make unrelated later writes look covered.
   */
  public void endCoveredWrite(final int previousMechanisms) {
    declaredCoverage = previousMechanisms;
  }

  /**
   * Tells whether EVERY modification this transaction made to this page was declared (see
   * {@link #beginCoveredWrite(int)}) as re-derivable by {@code mechanism}, so a commit-time conflict on the page can
   * be resolved by replaying that mechanism's tracked writes instead of failing the transaction.
   */
  public boolean isFullyCoveredBy(final int mechanism) {
    return (uncoveredMechanisms & mechanism) == 0;
  }

  @Override
  public void updateModifiedRange(final int start, final int end) {
    if (start < 0 || end >= getPhysicalSize())
      throw new IllegalArgumentException(
          "Update range (" + start + "-" + end + ") out of bound (0-" + (getPhysicalSize() - 1) + ")");

    // Whatever the current writer did NOT declare can no longer re-derive this page (issue #5596).
    uncoveredMechanisms |= COVERAGE_ALL_MERGES & ~declaredCoverage;

    if (modifiedRangeCount == 0) {
      modifiedRanges[0] = start;
      modifiedRanges[1] = end;
      modifiedRangeCount = 1;
      return;
    }

    // Locate the first interval that is not entirely before this one. Adjacency counts as an overlap: two intervals
    // separated by nothing are cheaper to keep as one than to ship with a second 24-byte WAL segment header.
    int i = 0;
    while (i < modifiedRangeCount && modifiedRanges[i * 2 + 1] < start - 1)
      ++i;

    if (i == modifiedRangeCount || modifiedRanges[i * 2] > end + 1) {
      insertModifiedRange(i, start, end);
      return;
    }

    // Absorb interval i, then coalesce every following interval this write bridged over.
    if (start < modifiedRanges[i * 2])
      modifiedRanges[i * 2] = start;

    int to = Math.max(modifiedRanges[i * 2 + 1], end);
    int j = i + 1;
    while (j < modifiedRangeCount && modifiedRanges[j * 2] <= to + 1) {
      to = Math.max(to, modifiedRanges[j * 2 + 1]);
      ++j;
    }
    modifiedRanges[i * 2 + 1] = to;

    if (j > i + 1) {
      System.arraycopy(modifiedRanges, j * 2, modifiedRanges, (i + 1) * 2, (modifiedRangeCount - j) * 2);
      modifiedRangeCount -= j - i - 1;
    }
  }

  /**
   * Inserts a new interval at position {@code index}, keeping the array sorted. When the budget is exhausted the two
   * closest intervals are merged: the gap between them is the only thing that gets shipped needlessly, so merging the
   * smallest one loses the least.
   */
  private void insertModifiedRange(final int index, final int start, final int end) {
    System.arraycopy(modifiedRanges, index * 2, modifiedRanges, (index + 1) * 2, (modifiedRangeCount - index) * 2);
    modifiedRanges[index * 2] = start;
    modifiedRanges[index * 2 + 1] = end;
    ++modifiedRangeCount;

    if (modifiedRangeCount <= MAX_MODIFIED_RANGES)
      return;

    int closest = 0;
    int smallestGap = Integer.MAX_VALUE;
    for (int k = 0; k < modifiedRangeCount - 1; ++k) {
      final int gap = modifiedRanges[(k + 1) * 2] - modifiedRanges[k * 2 + 1];
      if (gap < smallestGap) {
        smallestGap = gap;
        closest = k;
      }
    }

    modifiedRanges[closest * 2 + 1] = modifiedRanges[(closest + 1) * 2 + 1];
    System.arraycopy(modifiedRanges, (closest + 2) * 2, modifiedRanges, (closest + 1) * 2,
        (modifiedRangeCount - closest - 2) * 2);
    --modifiedRangeCount;
  }

  public WALFile getWALFile() {
    return walFile.get();
  }

  public void setWALFile(final WALFile WALFile) {
    this.walFile.set(WALFile);
  }

  /**
   * Atomically detaches the WAL file from this page, so the pending-flush ack can be released EXACTLY once
   * whichever of the racing paths (flush success, file-dropped flush branch, dropped-file batch purge) gets
   * here first. Returns {@code null} for every caller but the first.
   */
  public WALFile takeWALFile() {
    return walFile.getAndSet(null);
  }

  public void move(int startPosition, int destPosition, final int length) {
    if (length < 0)
      throw new IllegalArgumentException(
          "Cannot move a negative number of bytes in page " + pageId + " (startPosition=" + startPosition + " destPosition="
              + destPosition + " length=" + length + " pageSize=" + size + ")");
    startPosition += PAGE_HEADER_SIZE;
    destPosition += PAGE_HEADER_SIZE;
    if (length > 0)
      updateModifiedRange(Math.min(startPosition, destPosition), Math.max(startPosition, destPosition) + length - 1);
    content.move(startPosition, destPosition, length);
  }

  private void checkBoundariesOnWrite(final int start, final int length) {
    if (start < 0)
      throw new IllegalArgumentException("Invalid position " + start);

    if (start + length > getPhysicalSize())
      throw new IllegalArgumentException(
          "Cannot write outside the page space (" + (start + length) + ">" + getPhysicalSize() + ")");

    updateModifiedRange(start, start + length - 1);
  }
}
