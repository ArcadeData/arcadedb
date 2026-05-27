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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4371: FileManager mutates ArrayList from multiple threads
 * without synchronization, causing ConcurrentModificationException and torn reads.
 */
class FileManagerConcurrencyTest {

  private static final Set<String> FILE_EXT = FileManagerTest.FILE_EXT;

  /**
   * Verifies that iterating the list returned by {@link FileManager#getFiles()} does not
   * throw {@link java.util.ConcurrentModificationException} while concurrent writer threads
   * call {@link FileManager#newFileId()}, which structurally modifies the backing list.
   *
   * <p>Before the fix, {@code getFiles()} returned a live {@link Collections#unmodifiableList}
   * view of the internal {@link ArrayList}. Any structural modification on another thread
   * invalidated active iterators, producing a CME. After the fix, {@code getFiles()} returns
   * a defensive snapshot so iteration is always safe.
   */
  @Tag("slow")
  @Test
  void getFiles_noExceptionUnderConcurrentMutation(@TempDir Path dir) throws InterruptedException {
    final FileManager manager = new FileManager(dir.toAbsolutePath().toString(),
        ComponentFile.MODE.READ_WRITE, FILE_EXT);

    final int writerThreads = 4;
    final int readerThreads = 4;
    final int iterations = 500;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(writerThreads + readerThreads);
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    for (int t = 0; t < writerThreads; t++) {
      new Thread(() -> {
        try {
          startGate.await();
          for (int i = 0; i < iterations; i++)
            manager.newFileId();
        } catch (final Throwable e) {
          firstError.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "writer-" + t).start();
    }

    for (int t = 0; t < readerThreads; t++) {
      new Thread(() -> {
        try {
          startGate.await();
          for (int i = 0; i < iterations; i++) {
            // Iterate the returned list — must not throw ConcurrentModificationException
            for (final ComponentFile f : manager.getFiles()) {
              // touch each element so the loop body is not optimised away
              if (f != null)
                f.getFileId();
            }
          }
        } catch (final Throwable e) {
          firstError.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "reader-" + t).start();
    }

    startGate.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).as("threads completed within timeout").isTrue();
    assertThat(firstError.get()).as("no exception from concurrent access").isNull();
  }

  /**
   * Verifies that concurrent calls to {@link FileManager#newFileId()} return strictly unique,
   * non-negative IDs and that the list size after all calls equals the total number of
   * reservations made.
   */
  @Test
  void newFileId_returnsUniqueIdsUnderConcurrency(@TempDir Path dir) throws InterruptedException {
    final FileManager manager = new FileManager(dir.toAbsolutePath().toString(),
        ComponentFile.MODE.READ_WRITE, FILE_EXT);

    final int threads = 8;
    final int perThread = 200;
    final int total = threads * perThread;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final List<Integer> ids = Collections.synchronizedList(new ArrayList<>(total));
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        try {
          startGate.await();
          for (int i = 0; i < perThread; i++)
            ids.add(manager.newFileId());
        } catch (final Throwable e) {
          firstError.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "id-getter-" + t).start();
    }

    startGate.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).as("threads completed within timeout").isTrue();
    assertThat(firstError.get()).as("no exception during concurrent newFileId").isNull();

    assertThat(ids).hasSize(total);
    assertThat(ids.stream().distinct().count()).as("all returned IDs are unique").isEqualTo(total);
    assertThat(ids).allMatch(id -> id >= 0, "all IDs are non-negative");
    assertThat(manager.getFiles()).hasSize(total);
  }
}
