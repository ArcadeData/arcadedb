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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8254, second round (PR #8309 review): which RAM writes belong to a transaction, and which are applied at the
 * moment they run.
 * <ul>
 *   <li>A SET issued while a transaction is active - a MULTI/EXEC block's own, the one the HTTP command endpoint
 *   wraps every request in, or one the embedding application opened - is published only when THAT transaction
 *   commits, and discarded when it rolls back. Keyed to the transaction that really commits, not to "was one
 *   already active": the first cut fell back to writing straight through under the HTTP wrapper, so a failed
 *   {@code MULTI ... EXEC} sent over HTTP still left its SET applied.</li>
 *   <li>INCR/DECR and GETDEL answer with a value the caller acts on (a sequence number, a claimed token), so they
 *   are applied atomically to the shared map at the moment they run, like Redis and like a database sequence.
 *   Buffering them and replaying the increment at publish time let two concurrent callers receive the SAME
 *   number. The block's own retry reuses what its first attempt reserved instead of applying it again.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8254RedisRamTransactionScopeTest extends BaseRedisServerTest {

  /**
   * A block's INCR must hand out a number no concurrent INCR on the same key can also receive. The block is held
   * after its INCR ran (inside its HSET) while a standalone INCR runs on another thread.
   */
  @Test
  void incrInABlockNeverRepliesTheSameNumberAsAConcurrentIncr() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Order8254");

    final CountDownLatch blockInsideHset = new CountDownLatch(1);
    final CountDownLatch standaloneDone = new CountDownLatch(1);
    final BeforeRecordCreateListener pause = record -> {
      blockInsideHset.countDown();
      try {
        standaloneDone.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    };
    database.getSchema().getType("Order8254").getEvents().registerListener(pause);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<List<?>> block = executor.submit(() -> {
        try (final ResultSet rs = database.command("redis", """
            MULTI
            INCR seq
            HSET Order8254 {"id":1}
            EXEC
            """)) {
          return (List<?>) rs.next().getProperty("value");
        }
      });

      assertThat(blockInsideHset.await(30, TimeUnit.SECONDS)).isTrue();
      final long standaloneReply;
      try (final ResultSet rs = database.command("redis", "INCR seq")) {
        standaloneReply = ((Number) rs.next().getProperty("value")).longValue();
      } finally {
        standaloneDone.countDown();
      }

      final long blockReply = ((Number) block.get(30, TimeUnit.SECONDS).get(0)).longValue();

      assertThat(blockReply).as("two INCR calls on the same key must never receive the same number").isNotEqualTo(standaloneReply);
      assertThat(List.of(blockReply, standaloneReply)).containsExactlyInAnyOrder(1L, 2L);
      assertThat(getLong(database, "seq")).isEqualTo(2L);
    } finally {
      standaloneDone.countDown();
      executor.shutdownNow();
      database.getSchema().getType("Order8254").getEvents().unregisterListener(pause);
    }
  }

  /**
   * The issue's own repro: the block fails on a duplicated key on every attempt. Its SET is never published, and
   * its INCR is applied exactly once - by the first attempt, whose reservation the retry reuses - rather than once
   * per attempt.
   */
  @Test
  void failedBlockPublishesNoSetAndReservesItsIncrOnlyOnce() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Person8254");
    database.command("sql", "CREATE PROPERTY Person8254.name STRING");
    database.command("sql", "CREATE INDEX ON Person8254 (name) UNIQUE");
    database.transaction(() -> database.newDocument("Person8254").set("name", "dup").save());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("redis", """
          MULTI
          INCR c
          SET flag on
          HSET Person8254 {"name":"dup"}
          EXEC
          """)) {
        rs.next();
      }
    }).isInstanceOf(DuplicatedKeyException.class);

    assertThat(getLong(database, "c")).as("reserved by the first attempt, reused by the retry: not once per attempt").isEqualTo(1L);
    assertThat(get(database, "flag")).as("a SET in a block that never committed must not be published").isNull();
  }

  /**
   * Over HTTP the block joins the transaction the command endpoint wraps the request in, so its own
   * {@code database.transaction(...)} does not commit anything. The SET must still wait for the commit that
   * really happens - the wrapper's - and be discarded when the wrapper rolls back.
   */
  @Test
  void failedBlockOverHttpPublishesNoSet() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Account8254");
    database.command("sql", "CREATE PROPERTY Account8254.name STRING");
    database.command("sql", "CREATE INDEX ON Account8254 (name) UNIQUE");
    database.transaction(() -> database.newDocument("Account8254").set("name", "dup").save());

    assertThatThrownBy(() -> executeCommand(0, "redis", "MULTI\nSET flag on\nHSET Account8254 {\"name\":\"dup\"}\nEXEC"))
        .hasMessageContaining("Duplicated key");

    assertThat(get(database, "flag")).as("the HTTP wrapper rolled back: the block's SET must not be published").isNull();

    // The committed path over HTTP: the batch reads its own SET back after the block, then it is published.
    final Object reply = getResultValue(executeCommand(0, "redis", "MULTI\nSET flag on\nEXEC\nGET flag"));
    assertThat(reply.toString()).contains("on");
    assertThat(get(database, "flag")).isEqualTo("on");
  }

  /** A RAM write made inside a transaction the application opened is visible to that transaction and published at its commit. */
  @Test
  void setInsideAnApplicationTransactionFollowsItsOutcome() {
    final Database database = getServerDatabase(0, getDatabaseName());

    database.begin();
    try (final ResultSet rs = database.command("redis", "SET k rolledback")) {
      rs.next();
    }
    assertThat(get(database, "k")).as("read-your-writes inside the transaction").isEqualTo("rolledback");
    database.rollback();
    assertThat(get(database, "k")).as("discarded by the rollback").isNull();

    database.begin();
    try (final ResultSet rs = database.command("redis", "SET k committed")) {
      rs.next();
    }
    database.commit();
    assertThat(get(database, "k")).isEqualTo("committed");
  }

  /**
   * The block's own retry must not apply GETDEL twice either: the second attempt answers what the first claimed,
   * instead of reading the already deleted key back as null.
   */
  @Test
  void getdelInARetriedBlockAnswersTheClaimedValue() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Token8254");
    database.command("redis", "SET token abc");

    final int[] seen = { 0 };
    final BeforeRecordCreateListener failOnce = record -> {
      if (++seen[0] == 1)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #8254)");
      return true;
    };
    database.getSchema().getType("Token8254").getEvents().registerListener(failOnce);
    try (final ResultSet rs = database.command("redis", """
        MULTI
        GETDEL token
        HSET Token8254 {"id":1}
        EXEC
        """)) {
      final List<?> replies = (List<?>) rs.next().getProperty("value");
      assertThat(replies.get(0)).as("the retry reuses the value the first attempt claimed").isEqualTo("abc");
    } finally {
      database.getSchema().getType("Token8254").getEvents().unregisterListener(failOnce);
    }
    assertThat(get(database, "token")).isNull();
  }

  /**
   * A reserved variable name can never hold a value: a read of it answers "absent" on this engine as it does on the
   * RESP wire path ({@code LocalDatabase.getGlobalVariable}), while a write of it is still refused.
   */
  @Test
  void readOfAReservedNameAnswersAbsentLikeTheWirePath() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(get(database, "parent")).isNull();
    try (final ResultSet rs = database.command("redis", "EXISTS parent $current")) {
      assertThat(((Number) rs.next().getProperty("value")).intValue()).isZero();
    }
    try (final ResultSet rs = database.command("redis", "MULTI\nGET parent\nEXISTS parent\nEXEC")) {
      final List<?> replies = (List<?>) rs.next().getProperty("value");
      assertThat(replies.get(0)).isNull();
      assertThat(((Number) replies.get(1)).intValue()).isZero();
    }
    assertThatThrownBy(() -> database.command("redis", "SET parent 1")).hasMessageContaining("reserved");
  }

  private static Object get(final Database database, final String key) {
    try (final ResultSet rs = database.command("redis", "GET " + key)) {
      return rs.hasNext() ? rs.next().getProperty("value") : null;
    }
  }

  private static long getLong(final Database database, final String key) {
    return ((Number) get(database, key)).longValue();
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Redis Protocol Plugin needed for the module to be loaded
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
