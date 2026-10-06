/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A follower resync of a multi-GB database was bound by one thread compressing at the JDK default level, and a second
 * follower of the same database queued behind it on the per-database lock.
 * <ul>
 *   <li>the DEFLATE level of the snapshot ZIP is configurable and defaults to the fastest one;</li>
 *   <li>two followers of the same database are served concurrently on the point-in-time window path, which suspends
 *   nothing, and are still serialized on the flush-suspension fallback, which is what the lock is for (#5068).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9295SnapshotShipConcurrencyAndCompressionTest {
  private static final String DATABASE_PATH = "target/databases/snapshot-ship-concurrency-9295";

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void twoShipsOfTheSameDatabaseOverlapOnTheWindowPath() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    try (final Database database = createDatabase()) {
      assertThat(runTwoShips((DatabaseInternal) database)).as("both ships inside the streamer at the same time").isEqualTo(2);
    }
  }

  @Test
  void twoShipsOfTheSameDatabaseAreSerializedOnTheFallbackPath() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
    try (final Database database = createDatabase()) {
      assertThat(runTwoShips((DatabaseInternal) database)).as("the flush-suspension fallback must stay serialized")
          .isEqualTo(-1);
    }
  }

  @Test
  void snapshotCompressionLevelDefaultsToTheFastest() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_COMPRESSION_LEVEL.getDefValue()).isEqualTo(1);
  }

  @Test
  void theConfiguredLevelReachesTheZipStream() throws Exception {
    final byte[] payload = compressiblePayload();

    final byte[] stored = zip(payload, 0);
    final byte[] fast = zip(payload, 1);
    final byte[] defaulted = zip(payload, -1);

    assertThat(stored.length).as("level 0 stores").isGreaterThanOrEqualTo(payload.length);
    assertThat(fast.length).as("level 1 compresses").isLessThan(payload.length / 2);
    // a soft claim: tied to the seeded payload, DEFLATE does not guarantee it for arbitrary input
    assertThat(defaulted.length).as("-1 is the JDK default (6)").isLessThanOrEqualTo(fast.length);
    assertThat(unzip(stored)).isEqualTo(payload);
    assertThat(unzip(fast)).isEqualTo(payload);
  }

  @Test
  void anOutOfRangeLevelFallsBackToTheDefault() throws Exception {
    final byte[] payload = compressiblePayload();
    assertThat(zip(payload, 10)).hasSameSizeAs(zip(payload, 1));
    assertThat(zip(payload, -5)).hasSameSizeAs(zip(payload, 1));
    assertThat(unzip(zip(payload, 10))).isEqualTo(payload);
  }

  /**
   * @return 2 when both streamers were inside the callback at the same moment, 1 when never overlapped, or -1 when
   * they never overlapped AND the second ship was seen queued on the lock (positive proof of serialization).
   */
  private int runTwoShips(final DatabaseInternal db) throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final AtomicBoolean sawQueued = new AtomicBoolean();
    final AtomicBoolean overlapped = new AtomicBoolean();
    final AtomicInteger inside = new AtomicInteger();
    final AtomicInteger maxInside = new AtomicInteger();
    final CountDownLatch done = new CountDownLatch(2);

    for (int i = 0; i < 2; i++) {
      final Thread t = new Thread(() -> {
        try {
          SnapshotHttpHandler.streamThroughPointInTimeImage(db, db.getName(), null, lock, (image, pause) -> {
            maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
            try {
              final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
              // `overlapped` is sticky: the first thread to see both inside leaves, and the other must not then wait
              // for a second one that will never come
              while (!overlapped.get() && !sawQueued.get() && System.nanoTime() < deadline) {
                if (inside.get() == 2)
                  overlapped.set(true);
                if (lock.hasQueuedThreads())
                  sawQueued.set(true);
                Thread.sleep(5);
              }
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              inside.decrementAndGet();
            }
          });
        } finally {
          done.countDown();
        }
      });
      t.setDaemon(true);
      t.start();
    }
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    return maxInside.get() == 2 ? 2 : sawQueued.get() ? -1 : 1;
  }

  private static byte[] zip(final byte[] payload, final int level) throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_COMPRESSION_LEVEL, level);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = SnapshotHttpHandler.newSnapshotZipStream(baos, configuration)) {
      zos.putNextEntry(new ZipEntry("file.bin"));
      zos.write(payload);
      zos.closeEntry();
    }
    return baos.toByteArray();
  }

  private static byte[] unzip(final byte[] zipped) throws Exception {
    try (final ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zipped))) {
      assertThat(in.getNextEntry()).isNotNull();
      return in.readAllBytes();
    }
  }

  private static byte[] compressiblePayload() {
    final byte[] data = new byte[2 * 1024 * 1024];
    final Random random = new Random(42);
    for (int i = 0; i < data.length; i += 64)
      data[i] = (byte) random.nextInt(256);
    return data;
  }

  private static Database createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> database.newDocument("Doc").set("n", 1).save());
    return database;
  }
}
