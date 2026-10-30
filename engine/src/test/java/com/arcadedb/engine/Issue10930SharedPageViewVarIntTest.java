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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10930: the page view shared by every thread ({@link SharedImmutablePage}, #10654) did not override the
 * allocation-free varint read of #10656, {@code readNumber(int)} + {@code getLastReadNumberSize()}: the first moved the
 * position of the one shared buffer and the second answered a size kept on the page by the last call of ANY thread, so two
 * threads reading record headers from one cached page decoded each other's offsets. The pair is now position-free and
 * stateless ({@link BasePage#readNumber(int)} + {@link BasePage#readNumberSize(int)}), and the coverage test below makes
 * the next page read that moves the position, or keeps a result on the page, fail until the shared view answers it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10930SharedPageViewVarIntTest extends TestHelper {
  private static final int THREADS    = 4;
  private static final int ITERATIONS = 500_000;

  /**
   * Public reads of {@link BasePage} / {@link ImmutablePage} that a shared instance can answer as they are: each reads at an
   * absolute position, or builds a buffer of the caller's own, or reads only final/metadata fields. Anything else must be
   * overridden by {@link SharedImmutablePage}.
   */
  private static final Map<String, String> POSITION_FREE = Map.ofEntries(//
      Map.entry("getImmutableView(int,int)", "a slice over a duplicate of the buffer"),//
      Map.entry("modify()", "a copy of the array"),//
      Map.entry("getPhysicalSize()", "a final field"),//
      Map.entry("getMaxContentSize()", "a final field"),//
      Map.entry("getAvailableContentSize()", "the content size, metadata of the cached page"),//
      Map.entry("getContentSize()", "metadata of the cached page"),//
      Map.entry("getVersion()", "metadata of the cached page"),//
      Map.entry("readNumber(int)", "an absolute varint decode, #10930"),//
      Map.entry("readNumberSize(int)", "an absolute varint scan, #10930"),//
      Map.entry("readNumberAndSize(int)", "readNumber + readNumberSize"),//
      Map.entry("readLong(int)", "an absolute read"),//
      Map.entry("readInt(int)", "an absolute read"),//
      Map.entry("readFloat(int)", "an absolute read"),//
      Map.entry("readUnsignedInt(int)", "an absolute read"),//
      Map.entry("readShort(int)", "an absolute read"),//
      Map.entry("readUnsignedShort(int)", "an absolute read"),//
      Map.entry("readByte(int)", "an absolute read"),//
      Map.entry("readUnsignedByte(int)", "an absolute read"),//
      Map.entry("isSameContentAs(int,Binary,int,int)", "an array comparison"),//
      Map.entry("readString()", "delegates to readBytes(), which the shared view overrides"),//
      Map.entry("readString(int)", "delegates to readBytes(int), which the shared view overrides"),//
      Map.entry("getPageId()", "a final field"),//
      Map.entry("slice()", "an absolute slice, a buffer of the caller's own"),//
      Map.entry("equals(Object)", "the page id"),//
      Map.entry("hashCode()", "the page id"),//
      Map.entry("toString()", "the page id and version"));

  /** The instance state of a page: none of it is written by a read. A new field is per-read state until shown otherwise. */
  private static final Set<String> PAGE_FIELDS = Set.of("pageId", "content", "size", "version");

  @Test
  void manyThreadsReadingVarIntsFromTheSharedViewReadTheirOwn() throws Exception {
    final int pageSize = 4096;
    final Binary writer = new Binary(new byte[pageSize], pageSize);
    // TWO NUMBERS OF DIFFERENT ENCODED LENGTH: A STOLEN OFFSET CHANGES BOTH THE VALUE AND THE SIZE
    final int[] at = { 100, 200 };
    final long[] values = { 5L, 123_456_789_012L };
    final int[] sizes = new int[2];
    for (int i = 0; i < at.length; i++)
      sizes[i] = writer.putNumber(BasePage.PAGE_HEADER_SIZE + at[i], values[i]);
    assertThat(sizes[0]).isEqualTo(1);
    assertThat(sizes[1]).isEqualTo(6);

    final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), pageSize, writer.getContent(), 0,
        pageSize);
    final ImmutablePage shared = new CachedPage(page, true).getSharedImmutableView();
    assertThat(shared).isInstanceOf(SharedImmutablePage.class);

    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final List<Future<long[]>> results = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int first = t % 2;
        results.add(pool.submit(() -> {
          long wrongValues = 0;
          long wrongSizes = 0;
          for (int n = 0; n < ITERATIONS; n++) {
            final int i = (first + n) % 2;
            if (shared.readNumber(at[i]) != values[i])
              ++wrongValues;
            if (shared.readNumberSize(at[i]) != sizes[i])
              ++wrongSizes;
            final long[] pair = shared.readNumberAndSize(at[i]);
            if (pair[0] != values[i] || pair[1] != sizes[i])
              ++wrongValues;
          }
          return new long[] { wrongValues, wrongSizes };
        }));
      }
      for (final Future<long[]> result : results)
        assertThat(result.get(120, TimeUnit.SECONDS)).as("wrong values, wrong sizes").containsExactly(0L, 0L);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void readNumberDoesNotMoveThePosition() {
    final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), 1024);
    page.writeNumber(40, 123_456_789L);
    page.setBufferPosition(7);
    assertThat(page.readNumber(40)).isEqualTo(123_456_789L);
    assertThat(page.readNumberSize(40)).isEqualTo(Binary.getNumberSpace(123_456_789L));
    assertThat(page.readNumberAndSize(40)).containsExactly(123_456_789L, Binary.getNumberSpace(123_456_789L));
    assertThat(page.getBufferPosition()).isEqualTo(7);
  }

  @Test
  void readStringAtAnIndexReadsTheStringWrittenThere() {
    // readString(int) added the page header twice, once itself and once through readBytes(int)
    final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), 1024);
    page.writeString(30, "arcade");
    assertThat(page.readString(30)).isEqualTo("arcade");
  }

  @Test
  void everyPageReadIsPositionFreeOrAnsweredByTheSharedView() {
    final Set<String> overridden = new TreeSet<>();
    for (final Method m : SharedImmutablePage.class.getDeclaredMethods())
      if (Modifier.isPublic(m.getModifiers()))
        overridden.add(signature(m));

    final Set<String> uncovered = new TreeSet<>();
    final Set<String> seen = new TreeSet<>();
    for (final Class<?> c : List.of(BasePage.class, ImmutablePage.class))
      for (final Method m : c.getDeclaredMethods()) {
        if (!Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || m.isSynthetic())
          continue;
        final String signature = signature(m);
        seen.add(signature);
        if (!overridden.contains(signature) && !POSITION_FREE.containsKey(signature))
          uncovered.add(signature);
      }

    assertThat(uncovered).as(
        "public page methods neither overridden by SharedImmutablePage nor listed as position-free: a shared instance would answer "
            + "them on the one buffer every thread reads").isEmpty();
    // A STALE ENTRY OF THE ALLOW-LIST IS A METHOD THAT WAS REMOVED OR RENAMED: KEEP THE LIST HONEST
    assertThat(seen).as("allow-listed methods that no longer exist").containsAll(POSITION_FREE.keySet());

    final Set<String> fields = new TreeSet<>();
    for (final Class<?> c : List.of(BasePage.class, ImmutablePage.class, SharedImmutablePage.class))
      for (final Field f : c.getDeclaredFields())
        if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic())
          fields.add(f.getName());
    assertThat(fields).as("instance fields of a page: a new one kept by a read is a channel threads of a shared view would share")
        .isEqualTo(new TreeSet<>(PAGE_FIELDS));
  }

  private static String signature(final Method m) {
    final StringBuilder b = new StringBuilder(m.getName()).append('(');
    final Class<?>[] params = m.getParameterTypes();
    for (int i = 0; i < params.length; i++) {
      if (i > 0)
        b.append(',');
      b.append(params[i].getSimpleName());
    }
    return b.append(')').toString();
  }
}
