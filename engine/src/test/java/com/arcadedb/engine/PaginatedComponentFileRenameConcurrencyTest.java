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

import com.arcadedb.database.BasicDatabase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4546: {@code PaginatedComponentFile.rename} swaps the underlying
 * {@code FileChannel} (close -&gt; move -&gt; reopen) without any lock that blocks concurrent
 * page flush ({@code write}) or load ({@code read}). An in-flight I/O operation overlapping a
 * rename used to observe a closed/null channel and throw
 * {@code IllegalArgumentException("... is closed")} or {@code NullPointerException}.
 *
 * The test drives concurrent writers and readers against a real {@code PaginatedComponentFile}
 * while a rename loop swaps the channel underneath them and asserts that no such exception
 * escapes and that every I/O ultimately succeeds.
 */
class PaginatedComponentFileRenameConcurrencyTest {

  private static final int PAGE_SIZE     = 1024;
  private static final int FILE_ID       = 1;
  private static final int PAGE_COUNT    = 8;
  private static final int RENAME_CYCLES = 60;

  @TempDir
  Path tempDir;

  private PaginatedComponentFile pcf;
  private BasicDatabase          db;

  @BeforeEach
  void setUp() throws IOException {
    db = Mockito.mock(BasicDatabase.class);
    final String filePath = tempDir.resolve("page." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString();
    pcf = new PaginatedComponentFile(filePath, ComponentFile.MODE.READ_WRITE);

    // Pre-populate all pages so reads have content to fetch.
    for (int p = 0; p < PAGE_COUNT; p++) {
      final byte[] data = new byte[PAGE_SIZE];
      Arrays.fill(data, (byte) (0x10 + p));
      pcf.write(new MutablePage(new PageId(db, FILE_ID, p), PAGE_SIZE, data, 1, PAGE_SIZE));
    }
    pcf.force(true);
  }

  @AfterEach
  void tearDown() {
    if (pcf != null)
      pcf.close();
  }

  @Test
  void concurrentIoDuringRenameDoesNotSeeClosedChannel() throws Exception {
    final AtomicBoolean    stop       = new AtomicBoolean(false);
    final List<Throwable>  failures   = new CopyOnWriteArrayList<>();
    final CountDownLatch   ready      = new CountDownLatch(2);
    final CountDownLatch   go         = new CountDownLatch(1);

    final Thread writer = new Thread(() -> {
      ready.countDown();
      await(go);
      int p = 0;
      while (!stop.get()) {
        try {
          final byte[] data = new byte[PAGE_SIZE];
          Arrays.fill(data, (byte) (0x40 + (p % PAGE_COUNT)));
          pcf.write(new MutablePage(new PageId(db, FILE_ID, p % PAGE_COUNT), PAGE_SIZE, data, 2, PAGE_SIZE));
          p++;
        } catch (final Throwable t) {
          failures.add(t);
          return;
        }
      }
    }, "writer");

    final Thread reader = new Thread(() -> {
      ready.countDown();
      await(go);
      int p = 0;
      while (!stop.get()) {
        try {
          final CachedPage page = new CachedPage((PageManager) null, new PageId(db, FILE_ID, p % PAGE_COUNT), PAGE_SIZE);
          pcf.read(page);
          p++;
        } catch (final Throwable t) {
          failures.add(t);
          return;
        }
      }
    }, "reader");

    writer.start();
    reader.start();
    await(ready);
    go.countDown();

    // Rename loop on the main thread: the file is swapped between two physical names repeatedly.
    final Path nameA = pcf.getOSFile().toPath();
    final Path nameB = tempDir.resolve("renamed." + FILE_ID + "." + PAGE_SIZE + ".v0.arc");
    String current = nameA.toString();
    final String other = nameB.toString();
    try {
      for (int i = 0; i < RENAME_CYCLES && failures.isEmpty(); i++) {
        final String target = current.equals(nameA.toString()) ? other : nameA.toString();
        pcf.rename(target);
        current = target;
        Thread.yield();
      }
    } finally {
      stop.set(true);
      writer.join(10_000);
      reader.join(10_000);
    }

    if (!failures.isEmpty())
      throw new AssertionError("Concurrent I/O during rename observed a closed/invalid channel: " + failures.getFirst(), failures.getFirst());

    assertThat(failures).isEmpty();
    assertThat(pcf.isOpen()).isTrue();
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
