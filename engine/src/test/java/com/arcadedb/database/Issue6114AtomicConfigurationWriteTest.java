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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6114: {@code configuration.json} is written atomically, so a concurrent reader can never observe it
 * truncated.
 * <p>
 * {@code LocalDatabase.saveConfiguration()} used a plain {@code FileOutputStream}, which truncates the file to zero
 * and only then refills it. Anything reading it by name in that window - the full backup's lock-free t0 capture
 * above all - could see an empty or half-written document, and a crash there left the database without its
 * settings file.
 * <p>
 * The schema half of the same issue ({@code schema.json} / {@code schema.prev.json}) is covered by
 * {@code com.arcadedb.schema.Issue6114AtomicSchemaWriteTest}; this class deliberately does not repeat it.
 * <p>
 * Killing the JVM mid-write is not something a unit test can do, so the equivalent observation is made from another
 * thread: a reader polling the file as fast as it can, while it is rewritten over and over, must never see it
 * missing, empty, or unparseable. Against the previous code this fails within a handful of iterations.
 */
class Issue6114AtomicConfigurationWriteTest extends TestHelper {

  /** Enough saves that a poller running flat out lands inside a write window many times over. */
  private static final int SAVES = 300;

  @Test
  void configurationJsonIsNeverObservablyAbsentOrPartial() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final File configurationFile = local.getConfigurationFile();

    final List<String> defects = pollWhile(configurationFile, () -> {
      for (int i = 0; i < SAVES; i++)
        local.saveConfiguration();
    });

    assertThat(defects).as("configuration.json must never be observed absent or half-written").isEmpty();
    assertThat(configurationFile).exists();
    assertThat(configurationFile.getParentFile().list((dir, name) -> name.startsWith(LocalDatabase.CONFIGURATION_FILE_NAME)
        && name.endsWith(".tmp"))).as("the atomic write must not leave its scratch file behind").isEmpty();
  }

  /**
   * Runs {@code writer} on this thread while another thread reads {@code file} in a tight loop, and returns one
   * description per defective observation: the file missing, empty, or not a parseable JSON document.
   */
  private List<String> pollWhile(final File file, final ThrowingRunnable writer) throws Exception {
    final List<String> defects = new ArrayList<>();
    final AtomicBoolean polling = new AtomicBoolean(true);
    final AtomicLong reads = new AtomicLong();
    final AtomicReference<Exception> pollerFailure = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);

    final Thread poller = new Thread(() -> {
      started.countDown();
      while (polling.get()) {
        try {
          if (!file.exists()) {
            synchronized (defects) {
              defects.add("absent");
            }
            continue;
          }
          final String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
          reads.incrementAndGet();
          if (content.isEmpty())
            synchronized (defects) {
              defects.add("empty");
            }
          else
            try {
              new JSONObject(content);
            } catch (final RuntimeException e) {
              synchronized (defects) {
                defects.add("unparseable (" + content.length() + " chars): " + e.getMessage());
              }
            }
        } catch (final java.nio.file.NoSuchFileException e) {
          synchronized (defects) {
            defects.add("vanished between exists() and read");
          }
        } catch (final Exception e) {
          pollerFailure.compareAndSet(null, e);
          return;
        }
      }
    }, "issue6114-config-poller");
    poller.setDaemon(true);
    poller.start();

    try {
      assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
      writer.run();
    } finally {
      polling.set(false);
      poller.join(30_000);
    }

    assertThat(pollerFailure.get()).isNull();
    assertThat(reads.get()).as("the poller must actually have read the file while it was being rewritten")
        .isGreaterThan(10);

    synchronized (defects) {
      return new ArrayList<>(defects);
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
