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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
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
 * Issue #6114, first half: the two configuration files are written atomically, so a concurrent reader can never
 * observe one absent or partially written.
 * <p>
 * {@code LocalSchema.update()} used to rename {@code schema.json} aside and only then write the new one. Between
 * those two statements the file DID NOT EXIST, and while the writer ran it was truncated - a hazard independent of
 * backup (a crash there left a database whose schema file was missing) and the reason the backup could not simply
 * copy the file without a lock. {@code LocalDatabase.saveConfiguration()} had the same shape, truncating
 * {@code configuration.json} before rewriting it.
 * <p>
 * Killing the JVM between the two statements is not something a unit test can do, so the equivalent observation is
 * made from another thread: a reader polling the file as fast as it can, while the schema is saved over and over,
 * must never see it missing and must never see anything but a complete JSON document. Against the previous code
 * this fails within a handful of iterations.
 */
class Issue6114AtomicConfigurationWriteTest extends TestHelper {

  /** Enough saves that a poller running flat out lands inside a write window many times over. */
  private static final int SAVES = 300;

  @Test
  void schemaJsonIsNeverObservablyAbsentOrPartial() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final File schemaFile = schema.getConfigurationFile();

    final List<String> defects = pollWhile(schemaFile, () -> {
      for (int i = 0; i < SAVES; i++) {
        schema.createDocumentType("Type" + i);
        schema.saveConfiguration();
      }
    });

    assertThat(defects).as("schema.json must never be observed absent or half-written").isEmpty();
    // NOT VACUOUS: the file really was rewritten while the poller was watching it
    assertThat(new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8)).getJSONObject("types")
        .keySet()).contains("Type0", "Type" + (SAVES - 1));
  }

  @Test
  void configurationJsonIsNeverObservablyPartial() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final File configurationFile = local.getConfigurationFile();

    final List<String> defects = pollWhile(configurationFile, () -> {
      for (int i = 0; i < SAVES; i++)
        local.saveConfiguration();
    });

    assertThat(defects).as("configuration.json must never be observed absent or half-written").isEmpty();
    assertThat(configurationFile).exists();
  }

  @Test
  void thePreviousSchemaIsStillKeptAndIsAlwaysComplete() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final File schemaFile = schema.getConfigurationFile();
    final File previousFile = new File(schemaFile.getParentFile(), LocalSchema.SCHEMA_PREV_FILE_NAME);

    // createDocumentType saves the schema itself, so the content is sampled AFTER it and the save under test is the
    // explicit one below - otherwise schema.prev.json would be compared against a state two writes old
    schema.createDocumentType("Before");
    final String contentBeforeTheSaveUnderTest = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);

    schema.saveConfiguration();

    assertThat(previousFile).as("schema.prev.json is the recovery fallback and must still be maintained").exists();
    assertThat(Files.readString(previousFile.toPath(), StandardCharsets.UTF_8))
        .as("it must hold the schema as it was before the save, byte for byte")
        .isEqualTo(contentBeforeTheSaveUnderTest);
    assertThat(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8))
        .as("and the live file must have moved on, so the comparison above is not vacuous")
        .isNotEqualTo(contentBeforeTheSaveUnderTest);
    assertThat(new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8)).getJSONObject("types")
        .keySet()).contains("Before");
  }

  @Test
  void noTemporaryFileSurvivesTheWrite() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final File directory = schema.getConfigurationFile().getParentFile();

    for (int i = 0; i < 20; i++) {
      schema.createDocumentType("Leftover" + i);
      schema.saveConfiguration();
    }
    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).saveConfiguration();

    final String[] leftovers = directory.list((dir, name) -> name.endsWith(".tmp"));
    assertThat(leftovers).as("the atomic write must not leave its scratch file behind").isEmpty();
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
