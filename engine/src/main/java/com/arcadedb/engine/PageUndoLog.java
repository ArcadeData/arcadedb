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

import java.util.Arrays;

/**
 * What a statement overwrote on ONE page its transaction had already modified before the statement started, kept so the statement can
 * be taken back on its own (issue #10346).
 * <p>
 * A page the statement is the first to modify needs no log: dropping it from the transaction restores it. A page the transaction
 * already modified holds the earlier statements' work, which only a log of the bytes the statement overwrote can give back, so the
 * cost is bounded by the pages the statement re-touches and, within a page, by the granules it writes to.
 * <p>
 * The page is cut in granules of {@value #GRANULE_SIZE} bytes. The first write to a granule logs its bytes as they are, BEFORE the
 * write ({@link MutablePage#updateModifiedRange} is where every write of a page passes, ahead of the bytes it changes), and later
 * writes to the same granule log nothing: the first image is the only one a restore needs. A record insert therefore costs a few
 * granules however many fields it writes, and a page the statement writes all over costs at most the page.
 * <p>
 * Beside the bytes the log keeps what the page tracks about its own modifications (content size, modified ranges, coverage and
 * free-space bookkeeping), which the writes change as well and the commit reads.
 * <p>
 * Reused from statement to statement by its owner: {@link #begin} resets it. Not thread-safe, like the page: a transaction belongs to
 * one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PageUndoLog {
  static final int GRANULE_SHIFT = 8;
  static final int GRANULE_SIZE  = 1 << GRANULE_SHIFT;
  private static final int MAX_RETAINED_BYTES = 1 << 20;

  /** What the logs of one statement share, so a statement that rewrites the whole transaction cannot copy all of it. */
  public interface GrainBudget {
    /** @return false when the statement may not log another granule: the log then stops, and cannot restore the page */
    boolean takeGrain();
  }

  private       MutablePage       page;
  private final MutablePage.State state    = new MutablePage.State();
  private       long[]            logged   = new long[8];
  private       int[]             granules = new int[16];
  private       byte[]            data     = new byte[4 * GRANULE_SIZE];
  private       int               granuleCount;
  private       int               dataSize;
  private       GrainBudget       budget;
  private       boolean           overflowed;

  /**
   * Starts logging the writes of {@code target}. The page has to be one the transaction modified before the statement: its state at
   * this moment is the one {@link #restore()} gives back.
   */
  public void begin(final MutablePage target, final GrainBudget grainBudget) {
    this.page = target;
    this.budget = grainBudget;
    this.overflowed = false;
    target.captureState(state);
    final int words = ((target.getPhysicalSize() - 1) >>> GRANULE_SHIFT >>> 6) + 1;
    if (logged.length < words)
      logged = new long[words];
    else
      Arrays.fill(logged, 0, words, 0L);
    granuleCount = 0;
    dataSize = 0;
    target.setUndoLog(this);
  }

  /** Stops logging and lets go of the page: what was logged stays until the next {@link #begin}. */
  public void end() {
    if (page != null) {
      page.setUndoLog(null);
      page = null;
    }
    budget = null;
    // A bulk statement must not leave its buffers behind for the next one
    if (data.length > MAX_RETAINED_BYTES) {
      data = new byte[4 * GRANULE_SIZE];
      granules = new int[16];
    }
  }

  public MutablePage getPage() {
    return page;
  }

  /** Whether the budget ran out: what was logged does not describe the page any more, and {@link #restore()} must not be used. */
  public boolean isOverflowed() {
    return overflowed;
  }

  /** The granules logged so far. */
  public int getLoggedGranules() {
    return granuleCount;
  }

  /**
   * Called by the page BEFORE it overwrites the bytes {@code start..end} (inclusive, content offsets with the page header): logs the
   * granules of the range that no earlier write of the statement reached.
   */
  void beforeWrite(final int start, final int end) {
    if (overflowed)
      return;
    final byte[] array = page.content.getContent();
    final int base = page.content.getContentBeginOffset();
    final int last = Math.min(end, array.length - base - 1) >>> GRANULE_SHIFT;
    for (int granule = start >>> GRANULE_SHIFT; granule <= last; granule++) {
      final int word = granule >>> 6;
      final long bit = 1L << (granule & 63);
      if ((logged[word] & bit) != 0L)
        continue;
      if (budget != null && !budget.takeGrain()) {
        overflowed = true;
        return;
      }
      logged[word] |= bit;

      final int from = granule << GRANULE_SHIFT;
      final int length = Math.min(GRANULE_SIZE, array.length - base - from);
      if (dataSize + length > data.length)
        data = Arrays.copyOf(data, Math.max(data.length << 1, dataSize + length));
      if (granuleCount == granules.length)
        granules = Arrays.copyOf(granules, granuleCount << 1);
      System.arraycopy(array, base + from, data, dataSize, length);
      granules[granuleCount++] = granule;
      dataSize += length;
    }
  }

  /**
   * Gives the page back the bytes and the bookkeeping it had when {@link #begin} was called. The log is spent afterwards: the page
   * is the statement's no more.
   */
  public void restore() {
    final byte[] array = page.content.getContent();
    final int base = page.content.getContentBeginOffset();
    int offset = 0;
    for (int i = 0; i < granuleCount; i++) {
      final int from = granules[i] << GRANULE_SHIFT;
      final int length = Math.min(GRANULE_SIZE, array.length - base - from);
      System.arraycopy(data, offset, array, base + from, length);
      offset += length;
    }
    page.restoreState(state);
    Arrays.fill(logged, 0L);
    granuleCount = 0;
    dataSize = 0;
  }
}
