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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The insert path of a bucket answers "where is the next free slot and where does the content end" from a summary of
 * the record table that the transaction's own page keeps while it appends (see
 * {@link LocalBucket#getFreeSpaceInPage}), instead of walking every slot per insert. The summary must never outlive a
 * write that could change the answer: deletes, record growth that moves content, chunked records, and the reuse of
 * holes in the same or a later transaction. This test drives all of those against a model, reopens the database and
 * checks the model again.
 */
class BucketPackedScanTest extends TestHelper {
  private final Map<Integer, String> model = new HashMap<>();
  private final Map<Integer, RID>    rids  = new HashMap<>();
  private       int                  nextId = 0;

  @Test
  void appendsDeletesAndGrowthMatchAModelAcrossTransactionsAndReopen() {
    database.getSchema().createDocumentType("T", 1);
    final Random rnd = new Random(42);

    // 1) one long transaction of small appends (the summary path), several pages worth
    database.transaction(() -> {
      for (int i = 0; i < 6000; i++)
        insert(rnd, 0);
    });
    verifyModel();

    // 2) same-transaction mix: appends, deletes of records of this very transaction, appends again, a growing update
    database.transaction(() -> {
      for (int round = 0; round < 40; round++) {
        for (int i = 0; i < 50; i++)
          insert(rnd, 1);
        for (int i = 0; i < 20; i++)
          deleteRandom(rnd);
        for (int i = 0; i < 10; i++)
          growRandom(rnd);
      }
    });
    verifyModel();

    // 3) holes from earlier transactions get reused, interleaved with chunked records that span pages
    for (int tx = 0; tx < 6; tx++)
      database.transaction(() -> {
        for (int i = 0; i < 400; i++)
          insert(rnd, i % 97 == 0 ? 2 : 1);
        for (int i = 0; i < 300; i++)
          deleteRandom(rnd);
        for (int i = 0; i < 200; i++)
          insert(rnd, 0);
      });
    verifyModel();

    reopenDatabase();
    verifyModel();
    checkDatabaseIntegrity();
  }

  /** kind 0 = tiny, 1 = a few hundred bytes, 2 = larger than a page, so it is stored as a chunk chain */
  private void insert(final Random rnd, final int kind) {
    final int id = nextId++;
    final int len = switch (kind) {
      case 0 -> 1 + rnd.nextInt(12);
      case 1 -> 100 + rnd.nextInt(500);
      default -> 70_000 + rnd.nextInt(30_000);
    };
    final String payload = payload(rnd, len);
    final MutableDocument doc = database.newDocument("T").set("id", id, "pad", payload);
    doc.save();
    model.put(id, payload);
    rids.put(id, doc.getIdentity());
  }

  private void deleteRandom(final Random rnd) {
    if (model.isEmpty())
      return;
    final ArrayList<Integer> keys = new ArrayList<>(model.keySet());
    final int id = keys.get(rnd.nextInt(keys.size()));
    database.deleteRecord(database.lookupByRID(rids.get(id), true));
    model.remove(id);
    rids.remove(id);
  }

  private void growRandom(final Random rnd) {
    if (model.isEmpty())
      return;
    final ArrayList<Integer> keys = new ArrayList<>(model.keySet());
    final int id = keys.get(rnd.nextInt(keys.size()));
    final String payload = payload(rnd, 300 + rnd.nextInt(900));
    final MutableDocument doc = database.lookupByRID(rids.get(id), true).asDocument().modify();
    doc.set("pad", payload).save();
    model.put(id, payload);
    rids.put(id, doc.getIdentity());
  }

  private static String payload(final Random rnd, final int len) {
    final StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++)
      sb.append((char) ('a' + rnd.nextInt(26)));
    return sb.toString();
  }

  private void verifyModel() {
    assertThat(database.countType("T", true)).isEqualTo(model.size());
    final Map<Integer, String> seen = new HashMap<>();
    database.iterateType("T", true).forEachRemaining(r -> {
      final Document d = r.asDocument();
      assertThat(seen.put(d.getInteger("id"), d.getString("pad"))).isNull();
    });
    assertThat(seen).isEqualTo(model);
    // every RID the model tracks still reads back its own record
    for (final Map.Entry<Integer, RID> e : rids.entrySet())
      assertThat(database.lookupByRID(e.getValue(), true).asDocument().getString("pad")).isEqualTo(model.get(e.getKey()));
  }
}
