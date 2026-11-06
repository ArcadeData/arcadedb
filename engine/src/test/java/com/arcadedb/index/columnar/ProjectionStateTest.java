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
package com.arcadedb.index.columnar;

import com.arcadedb.engine.block.BlockCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProjectionStateTest {
  private static final ColumnarSegmentView[] NO_OVERLAYS = new ColumnarSegmentView[0];

  @TempDir
  Path dir;

  private final Map<String, SegmentReader> readers        = new HashMap<>();
  private final Set<String>                manifestFiles  = new HashSet<>();
  private final Set<ProjectionState>       retained       = new LinkedHashSet<>();
  private final List<String>               deleted        = new ArrayList<>();
  private final AtomicInteger              lastReleases   = new AtomicInteger();
  private final AtomicInteger              segmentDeletes = new AtomicInteger();

  private SegmentReader segment(final long id) throws IOException {
    final String name = SegmentFormat.segmentFileName(id);
    new SegmentRoundTripTest.Data(50, id, 10).write(dir.resolve(name), SegmentRoundTripTest.ALL, SegmentRoundTripTest.BLOCK_ROWS, id);
    final SegmentReader reader = SegmentReader.open(dir.resolve(name), null);
    readers.put(name, reader);
    return reader;
  }

  private String file(final String name) throws IOException {
    Files.write(dir.resolve(name), new byte[] { 1 });
    return name;
  }

  /** What the projection does on a last release (R2): delete what neither the manifest nor a retained state names. */
  private final Consumer<ProjectionState> deleter = state -> {
    lastReleases.incrementAndGet();
    retained.remove(state);
    final Set<String> held = new HashSet<>();
    for (final ProjectionState other : retained)
      held.addAll(other.files());
    for (final String name : ColumnarStore.deleteUnreferenced(dir, manifestFiles, held)) {
      deleted.add(name);
      final SegmentReader reader = readers.remove(name);
      if (reader != null) {
        reader.close();
        reader.file().deleted();
        segmentDeletes.incrementAndGet();
      }
    }
  };

  private ProjectionState state(final long generation, final SegmentReader[] segments, final Set<String> files) {
    final ProjectionState state = new ProjectionState(generation, segments, NO_OVERLAYS, Map.of(), files, true, deleter);
    retained.add(state);
    return state;
  }

  @Test
  void segmentFilesOutliveReleaseUntilLastReader() throws IOException {
    final SegmentReader s1 = segment(1);
    final String seg1 = SegmentFormat.segmentFileName(1);
    final String dead1 = file(SegmentFormat.deadFileName(1, 1));
    final String dict = file("dict-11.1.dic");
    manifestFiles.addAll(Set.of(seg1, dead1, dict));
    final long cacheFileId = s1.file().cacheFileId();
    s1.column("brand").isPresent(0); // loads a presence block into the cache
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isPositive();

    final ProjectionState state = state(1, new SegmentReader[] { s1 }, Set.of(seg1, dead1, dict));
    assertThat(s1.file().referencesForTesting()).isEqualTo(2);
    assertThat(state.retain()).isSameAs(state); // a query's snapshot

    // a merge publishes a manifest without segment 1, and the projection drops its own reference
    manifestFiles.removeAll(Set.of(seg1, dead1));
    state.release();
    assertThat(lastReleases.get()).isZero();
    assertThat(dir.resolve(seg1)).exists();
    assertThat(dir.resolve(dead1)).exists();
    assertThat(s1.file().referencesForTesting()).isEqualTo(2);

    state.release(); // the snapshot closes
    assertThat(lastReleases.get()).isEqualTo(1);
    assertThat(deleted).containsExactlyInAnyOrder(seg1, dead1);
    assertThat(dir.resolve(seg1)).doesNotExist();
    assertThat(dir.resolve(dict)).exists();
    assertThat(segmentDeletes.get()).isEqualTo(1);
    assertThat(s1.file().referencesForTesting()).isZero();
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isZero();

    assertThatThrownBy(state::release).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(state::retain).isInstanceOf(IllegalStateException.class);
    assertThat(lastReleases.get()).isEqualTo(1);
  }

  @Test
  void stillRetainedStateKeepsItsFiles() throws IOException {
    final SegmentReader s = segment(1);
    final SegmentReader t = segment(2);
    final String segS = SegmentFormat.segmentFileName(1);
    final String segT = SegmentFormat.segmentFileName(2);
    final String deadS = file(SegmentFormat.deadFileName(1, 1));
    manifestFiles.add(segT);

    final ProjectionState older = state(1, new SegmentReader[] { s }, Set.of(segS));
    final ProjectionState newer = state(2, new SegmentReader[] { s, t }, Set.of(segS, deadS, segT));
    assertThat(s.file().referencesForTesting()).isEqualTo(3);

    older.release();
    assertThat(lastReleases.get()).isEqualTo(1);
    assertThat(deleted).isEmpty();
    assertThat(dir.resolve(segS)).exists();
    assertThat(dir.resolve(deadS)).exists();
    assertThat(s.file().referencesForTesting()).isEqualTo(2);

    newer.release();
    assertThat(lastReleases.get()).isEqualTo(2);
    assertThat(deleted).containsExactlyInAnyOrder(segS, deadS);
    assertThat(dir.resolve(segT)).exists();
    assertThat(s.file().referencesForTesting()).isZero();
    assertThat(t.file().referencesForTesting()).isEqualTo(1);
    t.close();
  }

  @Test
  void accessorsAnswerTheConstructorArguments() throws IOException {
    final SegmentReader s = segment(1);
    final GlobalDictionary brand = GlobalDictionary.createInMemory();
    final ColumnarOverlay overlay = new ColumnarOverlay(SegmentRoundTripTest.MAX_RECORDS_IN_PAGE, SegmentRoundTripTest.ALL, () -> 0);
    final ColumnarSegmentView[] overlays = { overlay.publish() };
    final Set<String> files = Set.of(SegmentFormat.segmentFileName(1));
    manifestFiles.addAll(files);
    final ProjectionState state = new ProjectionState(7, new SegmentReader[] { s }, overlays, Map.of("brand", brand), files, false,
        deleter);

    assertThat(state.generation()).isEqualTo(7);
    assertThat(state.segments()).containsExactly(s);
    assertThat(state.overlays()).containsExactly(overlays[0]);
    assertThat(state.dictionary("brand")).isSameAs(brand);
    assertThat(state.dictionary("tags")).isNull();
    assertThat(state.files()).isSameAs(files);
    assertThat(state.isUsable()).isFalse();
    state.release();
    s.close();
  }

  @Test
  void aReleasedSegmentFileIsRefused() throws IOException {
    final SegmentReader s = segment(1);
    s.close();
    assertThatThrownBy(() -> state(1, new SegmentReader[] { segment(2), s }, Set.of())).isInstanceOf(IllegalStateException.class);
    // the reference taken on the first segment before the failure is given back
    assertThat(readers.get(SegmentFormat.segmentFileName(2)).file().referencesForTesting()).isEqualTo(1);
  }

  @Test
  void lastReleaseRunsOnceUnderConcurrentReaders() throws Exception {
    final SegmentReader s = segment(1);
    manifestFiles.add(SegmentFormat.segmentFileName(1));
    final ProjectionState state = state(1, new SegmentReader[] { s }, Set.of(SegmentFormat.segmentFileName(1)));
    final int threads = 8;
    final CyclicBarrier start = new CyclicBarrier(threads);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicInteger failures = new AtomicInteger();
    for (int i = 0; i < threads; i++) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int n = 0; n < 20_000; n++) {
            state.retain();
            state.release();
          }
        } catch (final Throwable e) {
          failures.incrementAndGet();
        } finally {
          done.countDown();
        }
      });
      thread.start();
    }
    done.await();
    assertThat(failures.get()).isZero();
    assertThat(lastReleases.get()).isZero();
    assertThat(state.referencesForTesting()).isEqualTo(1);
    state.release();
    assertThat(lastReleases.get()).isEqualTo(1);
    assertThat(s.file().referencesForTesting()).isEqualTo(1);
    s.close();
  }
}
