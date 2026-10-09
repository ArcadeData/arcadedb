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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8859 (follow-up of #8855): an {@code INSERT ... ON DUPLICATE KEY SKIP} racing a {@code CREATE INDEX} or a
 * {@code DROP INDEX} on the same type used to fail with an IndexException, because {@code SaveElementStep} asked
 * {@code TypeIndex.isUnique()} of an index with no sub-index yet, or one already invalidated.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8859SaveElementStepIndexDdlTest extends TestHelper {
  private static final int WRITERS = 4;
  private static final int ROUNDS  = 120;

  @Test
  void isUniqueIfPresentAnswersFalseForAnIndexThatWentAwayInsteadOfFailing() {
    final DocumentType type = database.getSchema().createDocumentType("Product");
    type.createProperty("sku", Type.STRING);
    final TypeIndex unique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "sku");
    assertThat(unique.isUniqueIfPresent()).isTrue();

    unique.drop();

    assertThat(unique.isUniqueIfPresent()).as("dropped: no index, so no uniqueness to enforce").isFalse();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void insertOnDuplicateKeySkipKeepsWorkingWhileTheUniqueIndexIsCreatedAndDropped() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.sku STRING");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong inserted = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final List<Thread> writers = new ArrayList<>();
    for (int t = 0; t < WRITERS; t++) {
      final int writer = t;
      final Thread thread = new Thread(() -> {
        long sequence = 0;
        while (!stop.get()) {
          try {
            // a key no other writer and no earlier round ever uses: nothing can clash, so every failure is a race
            final String sku = "w" + writer + "-" + sequence++;
            database.transaction(() -> database.command("sql",
                "INSERT INTO Product CONTENT [ {\"sku\":\"" + sku + "\"}, {\"sku\":\"" + sku + "-b\"} ] ON DUPLICATE KEY SKIP")
                .close());
            inserted.addAndGet(2);
          } catch (final Throwable e) {
            failures.computeIfAbsent(describe(e), k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      writers.add(thread);
      thread.start();
    }

    try {
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE INDEX ON Product (sku) UNIQUE");
        database.command("sql", "DROP INDEX `Product[sku]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : writers)
        thread.join();
    }

    assertThat(inserted.get()).isGreaterThan(0);
    assertThat(failures).as("inserts that succeeded: " + inserted.get()).isEmpty();
  }

  /** The exception and the engine frames it came from, so a failure names the code that raced. */
  private static String describe(final Throwable e) {
    final StringBuilder text = new StringBuilder(e.getClass().getSimpleName()).append(": ")
        .append(String.valueOf(e.getMessage()).split("\n")[0]);
    int frames = 0;
    for (final StackTraceElement frame : e.getStackTrace())
      if (frame.getClassName().startsWith("com.arcadedb") && frames++ < 6)
        text.append("\n   at ").append(frame.getClassName().substring(frame.getClassName().lastIndexOf('.') + 1)).append('.')
            .append(frame.getMethodName()).append(':').append(frame.getLineNumber());
    return text.toString();
  }
}
