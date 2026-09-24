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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8254: {@code RedisQueryEngine.executeTransaction()} (MULTI/EXEC) runs the queued commands inside
 * {@code database.transaction(...)}, which retries the whole block on an MVCC conflict or a duplicated key. The
 * RAM commands (SET/GET/GETDEL/INCR/DECR and their variants) do not write to the transaction at all: they write
 * straight to {@code database}'s global-variables map, a plain {@code ConcurrentHashMap} nothing rolls back. So a
 * retry caused by a document write elsewhere in the same block re-applied the discarded attempt's RAM mutations
 * too - an {@code INCR} in the same MULTI/EXEC block as a write that conflicts once would count twice for one
 * logical EXEC, even though the caller sees the retry succeed exactly once.
 * <p>
 * Driven directly via {@code database.command("redis", ...)} - the same reasoning {@link
 * Issue8037RedisRetryReplyDuplicationTest} documents at length: called this way, with nothing else on the thread
 * holding a transaction open, {@code executeTransaction()}'s own {@code database.transaction(...)} call is the
 * genuinely outermost one, so its retry loop is live (over the HTTP command endpoint, {@code
 * DatabaseAbstractHandler.executeInTransaction}'s own outer retry would make this rewind from scratch with a
 * fresh RAM overlay each time and never reach the bug).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8254RedisRetryRamCommandDuplicationTest extends BaseRedisServerTest {

  /** Throws a {@link ConcurrentModificationException} the first time {@code seen} reaches {@code failAt}. */
  private static BeforeRecordCreateListener failNthCreate(final AtomicInteger seen, final int failAt) {
    return record -> {
      if (seen.incrementAndGet() == failAt)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #8254 repro)");
      return true;
    };
  }

  @Test
  void incrInTheSameBlockAsAConflictingWriteIsNotAppliedTwiceOnRetry() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordCreateListener listener = failNthCreate(seen, 2);
    database.getSchema().getType("Person").getEvents().registerListener(listener);
    try {
      final String transaction = """
          MULTI
          INCR seq
          HSET Person {"id":1}
          HSET Person {"id":2}
          EXEC
          """;

      try (final ResultSet rs = database.command("redis", transaction)) {
        final Result result = rs.next();
        final List<?> replies = (List<?>) result.getProperty("value");
        assertThat(replies).hasSize(3);
        assertThat(((Number) replies.get(0)).longValue())
            .as("INCR's own reply for the attempt that finally committed must be 1, not 2")
            .isEqualTo(1L);
      }
    } finally {
      database.getSchema().getType("Person").getEvents().unregisterListener(listener);
    }

    assertThat(countOf(database, "Person"))
        .as("the retried attempt must still have committed exactly 2 documents")
        .isEqualTo(2);

    try (final ResultSet rs = database.command("redis", "GET seq")) {
      final Object value = rs.next().getProperty("value");
      assertThat(((Number) value).longValue())
          .as("a MULTI/EXEC block retried once must still leave the counter at 1, not 2")
          .isEqualTo(1L);
    }
  }

  private long countOf(final Database database, final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
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
