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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9322: a MULTI/EXEC block (or a loose RAM command) retried by the transaction that owns the request - the HTTP
 * command endpoint's auto-commit wrapper - re-applied its INCR to the shared map and lost the value its GETDEL had
 * claimed, because the reservation lived in a local of one {@code executeTransaction} invocation and the retry builds
 * a new one. The reservation now lives in the retry scope of the transaction call that owns the retries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9322RedisRetriedByWrapperTest extends BaseRedisServerTest {

  /** Fails the first record create with the MVCC conflict the retry exists for. */
  private BeforeRecordCreateListener conflictOnce(final AtomicInteger creates) {
    return record -> {
      if (creates.getAndIncrement() == 0)
        throw new ConcurrentModificationException("forced conflict (issue #9322)");
      return true;
    };
  }

  private static Object get(final Database database, final String key) {
    try (final ResultSet rs = database.command("redis", "GET " + key)) {
      return rs.hasNext() ? rs.next().getProperty("value") : null;
    }
  }

  /** The shape the HTTP handler drives: the caller owns the transaction, redis joins it, the caller retries. */
  private List<?> runUnderRetryingOwner(final Database database, final String block) {
    final Object[] reply = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("redis", block)) {
        reply[0] = rs.next().getProperty("value");
      }
    }, false, 3);
    return (List<?>) reply[0];
  }

  @Test
  void incrInABlockRetriedByItsOwnerIsAppliedOnce() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Person9322");
    final AtomicInteger creates = new AtomicInteger();
    final BeforeRecordCreateListener listener = conflictOnce(creates);
    database.getSchema().getType("Person9322").getEvents().registerListener(listener);
    try {
      final List<?> reply = runUnderRetryingOwner(database, "MULTI\nINCR seq\nHSET Person9322 {\"id\":1}\nEXEC");
      assertThat(creates.get()).as("the block really was retried").isGreaterThan(1);
      assertThat(((Number) reply.get(0)).longValue()).isEqualTo(1L);
      assertThat(((Number) get(database, "seq")).longValue()).as("one logical EXEC advances the counter once").isEqualTo(1L);
    } finally {
      database.getSchema().getType("Person9322").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void getdelInABlockRetriedByItsOwnerAnswersTheClaimedValue() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Token9322");
    try (final ResultSet rs = database.command("redis", "SET token abc")) {
      rs.next();
    }
    final AtomicInteger creates = new AtomicInteger();
    final BeforeRecordCreateListener listener = conflictOnce(creates);
    database.getSchema().getType("Token9322").getEvents().registerListener(listener);
    try {
      final List<?> reply = runUnderRetryingOwner(database, "MULTI\nGETDEL token\nHSET Token9322 {\"id\":1}\nEXEC");
      assertThat(creates.get()).isGreaterThan(1);
      assertThat(reply.get(0)).as("the retry answers what the first attempt claimed, not null").isEqualTo("abc");
      assertThat(get(database, "token")).isNull();
    } finally {
      database.getSchema().getType("Token9322").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void looseIncrInABatchRetriedByItsOwnerIsAppliedOnce() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Loose9322");
    final AtomicInteger creates = new AtomicInteger();
    final BeforeRecordCreateListener listener = conflictOnce(creates);
    database.getSchema().getType("Loose9322").getEvents().registerListener(listener);
    try {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("redis", "INCR loose")) {
          rs.next();
        }
        database.newDocument("Loose9322").set("id", 1).save();
      }, false, 3);
      assertThat(creates.get()).isGreaterThan(1);
      assertThat(((Number) get(database, "loose")).longValue()).isEqualTo(1L);
    } finally {
      database.getSchema().getType("Loose9322").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void scopeEndsWithTheOwnerSoTheNextRequestIncrementsAgain() {
    final Database database = getServerDatabase(0, getDatabaseName());
    for (int i = 1; i <= 3; i++)
      assertThat(((Number) runUnderRetryingOwner(database, "MULTI\nINCR seq\nEXEC").get(0)).longValue()).isEqualTo(i);
  }

  /** The real endpoint: the same blocks, a conflict forced on the first attempt, answered once and applied once. */
  @Test
  void blockRetriedByTheHttpEndpointIsAppliedOnce() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Http9322");
    try (final ResultSet rs = database.command("redis", "SET token abc")) {
      rs.next();
    }
    final AtomicInteger creates = new AtomicInteger();
    final BeforeRecordCreateListener listener = conflictOnce(creates);
    database.getSchema().getType("Http9322").getEvents().registerListener(listener);
    try {
      final JSONObject response = executeCommand(0, "redis",
          "MULTI\nINCR seq\nGETDEL token\nHSET Http9322 {\"id\":1}\nEXEC");
      assertThat(creates.get()).as("the HTTP wrapper really retried").isGreaterThan(1);
      final JSONArray reply = (JSONArray) getResultValue(response);
      assertThat(reply.getLong(0)).isEqualTo(1L);
      assertThat(reply.getString(1)).isEqualTo("abc");
      assertThat(((Number) get(database, "seq")).longValue()).isEqualTo(1L);
      assertThat(get(database, "token")).isNull();
    } finally {
      database.getSchema().getType("Http9322").getEvents().unregisterListener(listener);
    }
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
