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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9837: two rules, broken by the same thing - the Redis surfaces built HDEL / HSET out of raw engine calls instead of out
 * of one piece that knows what the engine answered.
 * <ul>
 *   <li>the integer an HDEL replies is the number of records it REMOVED: {@code Record.delete()} discards the engine's answer, so
 *   a delete a {@code BeforeRecordDeleteListener} vetoed was counted as a deletion on every branch;</li>
 *   <li>a multi-key HSET / HDEL is one unit of work: the RESP wire path's HDEL by index key ran with no transaction (one implicit
 *   commit per key, no retry) and its transient HSET wrote through {@code setGlobalVariable}, which no transaction undoes.</li>
 * </ul>
 * Each test drives the RESP wire path (Jedis) and, where the command exists there, the {@code redis} query language as well, so
 * the two surfaces cannot answer differently about the same command.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9837RedisHDelAccountingAndAtomicityTest extends BaseRedisServerTest {
  private static final String TYPE = "W";

  // --- rule 1: the reply counts what the engine removed ---

  @Test
  void wireHDelByIndexKeyDoesNotCountAVetoedDelete() {
    final Database database = createRows();
    vetoPid(database, "k3");
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k2", "k3")).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void queryHDelByIndexKeyDoesNotCountAVetoedDelete() {
    final Database database = createRows();
    vetoPid(database, "k3");
    assertThat(queryHDel(database, "HDEL " + TYPE + "[pid] k1 k2 k3")).isEqualTo(2);
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void wireHDelByRidDoesNotCountAVetoedDelete() {
    final Database database = createRows();
    final String[] rids = ridsOf(database);
    vetoPid(database, "k3");
    try (final Jedis jedis = newClient()) {
      // persistent form: the RID is named by the bucket argument
      assertThat(jedis.hdel(getDatabaseName() + "." + rids[0], rids[1], rids[2])).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void wireTransientHDelByRidDoesNotCountAVetoedDelete() {
    final Database database = createRows();
    final String[] rids = ridsOf(database);
    vetoPid(database, "k3");
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName(), rids[0], rids[1], rids[2])).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void queryHDelByRidDoesNotCountAVetoedDelete() {
    final Database database = createRows();
    final String[] rids = ridsOf(database);
    vetoPid(database, "k3");
    assertThat(queryHDel(database, "HDEL " + String.join(" ", rids))).isEqualTo(2);
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void aVetoedDeleteIsNotCountedOnAnyBranchWhenEveryRecordIsVetoed() {
    final Database database = createRows();
    final String[] rids = ridsOf(database);
    database.getSchema().getType(TYPE).getEvents().registerListener((BeforeRecordDeleteListener) record -> false);
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k2", "k3")).isZero();
      assertThat(jedis.hdel(getDatabaseName() + "." + rids[0], rids[1], rids[2])).isZero();
      assertThat(jedis.hdel(getDatabaseName(), rids)).isZero();
    }
    assertThat(queryHDel(database, "HDEL " + TYPE + "[pid] k1 k2 k3")).isZero();
    assertThat(queryHDel(database, "HDEL " + String.join(" ", rids))).isZero();
    assertThat(countRows(database)).isEqualTo(3L);
  }

  // --- rule 2: a multi-key command is one unit of work ---

  @Test
  void wireHDelByIndexKeyThatFailsOnTheThirdKeyUndoesTheFirstTwo() {
    final Database database = createRows();
    database.getSchema().getType(TYPE).getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if ("k3".equals(record.asDocument().getString("pid")))
        throw new IllegalStateException("boom on k3");
      return true;
    });
    try (final Jedis jedis = newClient()) {
      assertThatThrownBy(() -> jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k2", "k3")).isInstanceOf(
          JedisDataException.class).hasMessageContaining("boom on k3");
    }
    assertThat(countRows(database)).as("k1 and k2 must not stay deleted behind an error reply").isEqualTo(3L);
  }

  @Test
  void queryHDelByIndexKeyThatFailsOnTheThirdKeyUndoesTheFirstTwo() {
    final Database database = createRows();
    database.getSchema().getType(TYPE).getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if ("k3".equals(record.asDocument().getString("pid")))
        throw new IllegalStateException("boom on k3");
      return true;
    });
    assertThatThrownBy(() -> queryHDel(database, "HDEL " + TYPE + "[pid] k1 k2 k3")).hasStackTraceContaining("boom on k3");
    assertThat(countRows(database)).isEqualTo(3L);
  }

  @Test
  void wireHDelByIndexKeyIsRetriedAfterAConflictAndRepliesExactlyWhatItDeleted() {
    final Database database = createRows();
    final AtomicInteger seen = new AtomicInteger();
    database.getSchema().getType(TYPE).getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (seen.incrementAndGet() == 3)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #9837 repro)");
      return true;
    });
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k2", "k3")).isEqualTo(3L);
    }
    assertThat(countRows(database)).isZero();
  }

  @Test
  void wireHDelCountDoesNotAccumulateAcrossARetryThatAlsoHasAVeto() {
    final Database database = createRows();
    final AtomicInteger seen = new AtomicInteger();
    // the third delete of the first attempt conflicts, the retry then deletes k1 and k2 and k3 is vetoed
    database.getSchema().getType(TYPE).getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (seen.incrementAndGet() == 3)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #9837 repro)");
      return !"k3".equals(record.asDocument().getString("pid"));
    });
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k2", "k3")).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(1L);
  }

  @Test
  void theSameKeyTwiceInOneHDelIsCountedOnce() {
    final Database database = createRows();
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + TYPE + "[pid]", "k1", "k1", "k2")).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(1L);

    assertThat(queryHDel(database, "HDEL " + TYPE + "[pid] k3 k3")).isEqualTo(1);
    assertThat(countRows(database)).isZero();
  }

  @Test
  void theSameRidTwiceInOneHDelIsCountedOnce() {
    final Database database = createRows();
    final String[] rids = ridsOf(database);
    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hdel(getDatabaseName() + "." + rids[0], rids[0], rids[1], rids[1])).isEqualTo(2L);
    }
    assertThat(queryHDel(database, "HDEL " + rids[2] + " " + rids[2])).isEqualTo(1);
    assertThat(countRows(database)).isZero();
  }

  @Test
  void wireTransientHSetWithAMalformedElementStoresNothing() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    try (final Jedis jedis = newClient()) {
      assertThatThrownBy(() -> hSet(jedis, "{\"id\":\"v1\",\"n\":1}", "{\"id\":\"v2\",\"n\":2}", "{\"n\":3}")).isInstanceOf(
          JedisDataException.class).hasMessageContaining("'id' field");
      assertThat(database.getGlobalVariable("v1")).as("v1 must not stay stored behind an error reply").isNull();
      assertThat(database.getGlobalVariable("v2")).isNull();

      assertThatThrownBy(() -> hSet(jedis, "{\"id\":\"v1\"}", "{\"id\":\"v2\"}", "{not json")).isInstanceOf(JedisDataException.class);
      assertThat(database.getGlobalVariable("v1")).isNull();
      assertThat(database.getGlobalVariable("v2")).isNull();
    }
  }

  @Test
  void wireTransientHSetOfValidElementsStoresAllAndRepliesTheirCount() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    try (final Jedis jedis = newClient()) {
      assertThat(hSet(jedis, "{\"id\":\"v1\",\"n\":1}", "{\"id\":\"v2\",\"n\":2}", "{\"id\":\"v3\",\"n\":3}")).isEqualTo(3L);
      assertThat(database.getGlobalVariable("v1")).isNotNull();
      assertThat(database.getGlobalVariable("v2")).isNotNull();
      assertThat(database.getGlobalVariable("v3")).isNotNull();
      database.setGlobalVariable("v1", null);
      database.setGlobalVariable("v2", null);
      database.setGlobalVariable("v3", null);
    }
  }

  // --- helpers ---

  private Database createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".pid STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (pid) UNIQUE");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument(TYPE).set("pid", "k" + i).save();
    });
    return database;
  }

  private static String[] ridsOf(final Database database) {
    final String[] rids = new String[3];
    for (int i = 0; i < 3; i++)
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM " + TYPE + " WHERE pid = 'k" + (i + 1) + "'")) {
        rids[i] = rs.next().getProperty("r").toString();
      }
    return rids;
  }

  private static void vetoPid(final Database database, final String pid) {
    database.getSchema().getType(TYPE).getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> !pid.equals(record.asDocument().getString("pid")));
  }

  private static long countRows(final Database database) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static int queryHDel(final Database database, final String command) {
    try (final ResultSet rs = database.command("redis", command)) {
      return ((Number) rs.next().getProperty("value")).intValue();
    }
  }

  private long hSet(final Jedis jedis, final String... elements) {
    final String[] args = new String[elements.length + 1];
    args[0] = getDatabaseName();
    System.arraycopy(elements, 0, args, 1, elements.length);
    return ((Number) jedis.sendCommand(() -> "HSET".getBytes(StandardCharsets.UTF_8), args)).longValue();
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  private Jedis newClient() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }
}
