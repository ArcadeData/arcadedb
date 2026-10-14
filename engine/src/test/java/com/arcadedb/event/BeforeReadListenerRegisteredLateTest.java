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
package com.arcadedb.event;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A scan hands the BEFORE READ events to a bucket that skips finding the registry of the record's type until a listener was
 * registered anywhere in the JVM. A listener registered after the database was already read, on the database or on a type,
 * must still see every later record, whichever way it is read.
 */
class BeforeReadListenerRegisteredLateTest extends TestHelper {

  @Test
  void aTypeListenerRegisteredAfterAScanSeesTheNextScanAndLookup() {
    database.getSchema().createDocumentType("Late");
    final RID[] first = new RID[1];
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument d = database.newDocument("Late").set("id", i).save();
        if (i == 0)
          first[0] = d.getIdentity();
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Late")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(100L);
    }

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordReadListener listener = rid -> {
      seen.incrementAndGet();
      return true;
    };
    database.getSchema().getType("Late").getEvents().registerListener(listener);
    try {
      assertThat(RecordEventsRegistry.isBeforeReadListenerEverRegistered()).isTrue();

      try (final ResultSet rs = database.query("sql", "SELECT id FROM Late")) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).isEqualTo(100);
      }
      assertThat(seen.get()).isEqualTo(100);

      assertThat(database.lookupByRID(first[0], true)).isNotNull();
      assertThat(seen.get()).isEqualTo(101);
    } finally {
      database.getSchema().getType("Late").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void aDatabaseListenerRegisteredAfterAScanCanHideTheRecords() {
    database.getSchema().createDocumentType("Late2");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Late2").set("id", i).save();
    });
    assertThat(database.countType("Late2", false)).isEqualTo(50L);

    final BeforeRecordReadListener hideAll = rid -> false;
    database.getEvents().registerListener(hideAll);
    try {
      assertThat(RecordEventsRegistry.isBeforeReadListenerEverRegistered()).isTrue();
      try (final ResultSet rs = database.query("sql", "SELECT id FROM Late2")) {
        assertThat(rs.hasNext()).isFalse();
      }
    } finally {
      database.getEvents().unregisterListener(hideAll);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Late2")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(50L);
    }
  }
}
