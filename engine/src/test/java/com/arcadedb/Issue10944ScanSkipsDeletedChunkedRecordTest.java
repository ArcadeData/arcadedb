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
package com.arcadedb;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #10944: a scan must skip a multi-page (chunked) record that a concurrent transaction deletes while the scan
 * is reading it, instead of failing the whole query with "Multi-page record was deleted during read".
 */
@Tag("slow")
class Issue10944ScanSkipsDeletedChunkedRecordTest extends TestHelper {
  private static final String TYPE         = "Chunked";
  private static final int    PAYLOAD_SIZE = 200_000; // SEVERAL 64KB PAGES: STORED AS A CHUNK CHAIN
  private static final int    SEED         = 200;
  private static final int    ROUNDS       = 400;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void scanSkipsChunkedRecordsDeletedWhileReading() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE).createProperty("payload", Type.STRING);
    });

    final String payload = "x".repeat(PAYLOAD_SIZE);
    database.transaction(() -> {
      for (int i = 0; i < SEED; i++)
        database.newDocument(TYPE).set("payload", payload).save();
    });

    final AtomicBoolean stop = new AtomicBoolean(false);
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final AtomicInteger scans = new AtomicInteger();

    final List<Thread> scanners = new ArrayList<>();
    for (int t = 0; t < 4; t++) {
      final Thread scanner = new Thread(() -> {
        while (!stop.get()) {
          try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
            while (rs.hasNext()) {
              final Result r = rs.next();
              // A RETURNED RECORD IS ALWAYS A WHOLE ONE
              assertThat(r.<String>getProperty("payload").length()).isIn(PAYLOAD_SIZE, PAYLOAD_SIZE + 70_000);
            }
            scans.incrementAndGet();
          } catch (final Throwable e) {
            failures.add(e);
            return;
          }
        }
      }, "scanner-" + t);
      scanners.add(scanner);
      scanner.start();
    }

    // WRITER: INSERT A BATCH, GROW SOME, DELETE THE PREVIOUS BATCH
    final String bigger = "y".repeat(PAYLOAD_SIZE + 70_000);
    List<RID> previous = new ArrayList<>();
    for (int round = 0; round < ROUNDS && failures.isEmpty(); round++) {
      final List<RID> created = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final MutableDocument d = database.newDocument(TYPE).set("payload", payload);
          d.save();
          created.add(d.getIdentity());
        }
      });
      final List<RID> toDelete = previous;
      database.transaction(() -> {
        for (final RID rid : toDelete)
          database.deleteRecord(rid.asDocument());
      });
      database.transaction(() -> {
        if (!created.isEmpty()) {
          final MutableDocument d = created.get(0).asDocument().modify();
          d.set("payload", bigger);
          d.save();
        }
      });
      previous = created;
    }

    stop.set(true);
    for (final Thread scanner : scanners) {
      scanner.join(60_000);
      assertThat(scanner.isAlive()).as("scanner %s did not finish", scanner.getName()).isFalse();
    }

    assertThat(failures).as("scans failed: %s", failures.stream().map(Issue10944ScanSkipsDeletedChunkedRecordTest::describe).toList())
        .isEmpty();
    assertThat(scans.get()).isGreaterThan(0);
  }

  private static String describe(final Throwable e) {
    final StringBuilder chain = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      final String message = t.getMessage() == null ? "" : t.getMessage();
      chain.append(t.getClass().getSimpleName()).append(": ").append(message, 0, Math.min(200, message.length())).append(" <- ");
    }
    return chain.toString();
  }
}
