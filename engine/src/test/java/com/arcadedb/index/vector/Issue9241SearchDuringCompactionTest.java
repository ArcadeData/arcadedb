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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9241: while COMPACT INDEX builds its graph, a search missed every record added since the last graph build (the swap
 * emptied the delta buffer, and the new graph is published only when the build ends) and, once deletes made the rewrite
 * renumber the ids, nearly every record (the resident graph's ordinal map still held the old ids).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue9241SearchDuringCompactionTest extends TestHelper {
  private static final int DIM    = 32;
  private static final int BASE   = 12_000;
  private static final int RECENT = 100;

  @Test
  void recentRecordsAreFoundWhileTheCompactionBuilds() throws Exception {
    final List<RID> all = load(0);
    assertThat(searchMisses(all.subList(BASE, all.size()), BASE)).isEmpty();
    searchWhileCompacting(all, BASE);
  }

  @Test
  void recordsAreFoundWhileACompactionThatRenumbersTheIdsBuilds() throws Exception {
    final List<RID> all = load(600);
    searchWhileCompacting(all, BASE);
  }

  /** Creates the type, builds a graph over BASE records, deletes some, then adds RECENT records one per transaction. */
  private List<RID> load(final int deletes) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE V");
      database.command("sql", "CREATE PROPERTY V.id LONG");
      database.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON V (emb) LSM_VECTOR METADATA { \"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"mutationsBeforeRebuild\": 1000000 }");
    });
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < BASE; i++) {
        final var d = database.newDocument("V").set("id", (long) i).set("emb", vector(i));
        d.save();
        rids.add(d.getIdentity());
      }
    });
    try (final ResultSet rs = database.command("sql", "COMPACT INDEX `V[emb]`")) {
      while (rs.hasNext())
        rs.next();
    }
    if (deletes > 0)
      database.transaction(() -> {
        // the first records: none of them is among the ones searched
        for (int i = 0; i < deletes; i++)
          database.lookupByRID(rids.get(i), true).delete();
      });
    for (int i = BASE; i < BASE + RECENT; i++) {
      final int id = i;
      database.transaction(() -> {
        final var d = database.newDocument("V").set("id", (long) id).set("emb", vector(id));
        d.save();
        rids.add(d.getIdentity());
      });
    }
    return rids;
  }

  private void searchWhileCompacting(final List<RID> rids, final int firstRecent) throws Exception {
    // the 100 recent records and 100 older ones, none of them deleted
    final List<RID> probes = new ArrayList<>(rids.subList(firstRecent, rids.size()));
    probes.addAll(rids.subList(2000, 2000 + RECENT));
    final int[] probeIds = new int[probes.size()];
    for (int i = 0; i < RECENT; i++)
      probeIds[i] = firstRecent + i;
    for (int i = 0; i < RECENT; i++)
      probeIds[RECENT + i] = 2000 + i;

    final AtomicBoolean done = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread compactor = new Thread(() -> {
      try (final ResultSet rs = database.command("sql", "COMPACT INDEX `V[emb]`")) {
        while (rs.hasNext())
          rs.next();
      } catch (final Throwable e) {
        failure.set(e);
      } finally {
        done.set(true);
      }
    });
    compactor.start();

    int passesDuringTheCompaction = 0;
    final List<String> missing = new ArrayList<>();
    while (!done.get()) {
      final boolean running = !done.get();
      for (int i = 0; i < probes.size(); i++)
        if (!found(probeIds[i], probes.get(i)))
          missing.add("id=" + probeIds[i]);
        else if (!found(probeIds[i], probes.get(i), probes))
          // an allow-list (the ordinal map is read by vector id) holding the probes only
          missing.add("filtered id=" + probeIds[i]);
      if (running && !done.get())
        passesDuringTheCompaction++;
    }
    compactor.join();

    assertThat(failure.get()).as("the compaction failed").isNull();
    assertThat(passesDuringTheCompaction).as("search passes that ran entirely while the compaction was building").isGreaterThan(0);
    assertThat(missing).as("records not found with their own vector while the compaction was running").isEmpty();
    assertThat(searchMisses(probes, probeIds)).as("records not found after the compaction").isEmpty();
  }

  private List<String> searchMisses(final List<RID> probes, final int firstId) {
    final int[] ids = new int[probes.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = firstId + i;
    return searchMisses(probes, ids);
  }

  private List<String> searchMisses(final List<RID> probes, final int[] ids) {
    final List<String> missing = new ArrayList<>();
    for (int i = 0; i < probes.size(); i++)
      if (!found(ids[i], probes.get(i)))
        missing.add("id=" + ids[i]);
    return missing;
  }

  private boolean found(final int id, final RID rid) {
    return found(id, rid, null);
  }

  private boolean found(final int id, final RID rid, final List<RID> allowed) {
    final String sql = allowed == null ? "SELECT expand(vectorNeighbors('V[emb]', ?, 10, 200))" :
        "SELECT expand(vectorNeighbors('V[emb]', ?, 10, ?))";
    final Object[] args = allowed == null ? new Object[] { vector(id) } :
        new Object[] { vector(id), Map.of("efSearch", 200, "filter", allowed) };
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        if (r.getIdentity().isPresent() && r.getIdentity().get().equals(rid))
          return true;
      }
    }
    return false;
  }

  private static float[] vector(final long seed) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      long z = seed * 1000003L + i * 104729L + 12345;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      z ^= z >>> 31;
      v[i] = (float) ((z >>> 11) * (1.0 / (1L << 53)) * 2 - 1);
    }
    return v;
  }
}
