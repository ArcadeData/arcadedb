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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The engine-side half of issue #6965: a page version reserved by the replication layer counts as the most recent one
 * in the phase-1 check, a standalone database with no reservations installed is untouched, and the phase-2 bump never
 * consults the reservations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageVersionReservationsTest {
  @TempDir
  Path tempDir;

  private LocalDatabase db;
  private RID           rid;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("reservations").toString()).create();
    db.getSchema().createDocumentType("Counter", 1);
    db.transaction(() -> rid = db.newDocument("Counter").set("value", 0L).save().getIdentity());
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aReservedVersionRefusesThePhaseOneCheckUntilReleased() {
    assertThat(db.getPageVersionReservations()).as("a standalone database has none installed").isNull();

    final AtomicInteger consulted = new AtomicInteger();
    final PageId counterPage = new PageId(db, rid.getBucketId(), 0);
    db.setPageVersionReservations(pageId -> {
      consulted.incrementAndGet();
      return pageId.equals(counterPage) ? Integer.MAX_VALUE : -1;
    });

    assertThatThrownBy(() -> increment(0)).isInstanceOf(ConcurrentModificationException.class);
    assertThat(consulted.get()).as("the phase-1 check consulted the reservations").isGreaterThan(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isZero();

    // Released: the same transaction goes through, and the phase-2 bump validated against the local copy only.
    db.setPageVersionReservations(pageId -> -1);
    increment(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isEqualTo(1L);

    db.setPageVersionReservations(null);
    increment(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isEqualTo(2L);
  }

  private void increment(final int retries) {
    db.transaction(() -> {
      final Document counter = db.lookupByRID(rid, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
    }, false, retries);
  }
}
