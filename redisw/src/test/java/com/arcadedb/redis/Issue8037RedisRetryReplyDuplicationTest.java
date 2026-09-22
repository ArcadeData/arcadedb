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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8037: {@code RedisQueryEngine.executeTransaction()} (MULTI/EXEC), {@code hSet()} and {@code hDel()} each
 * built their reply into a collection or counter declared OUTSIDE the one-argument {@code database.transaction(...)}
 * they ran their work in, and appended to it from INSIDE. {@code database.transaction(...)} retries the block up to
 * {@code arcadedb.txRetries} times on a {@link ConcurrentModificationException} (or a duplicated key), rolling the
 * failed attempt back first - and nothing reset the accumulator between attempts, so a single retry published the
 * discarded attempt's replies alongside the committed ones: an EXEC of N commands answered more than N replies, and
 * HSET/HDEL answered a count larger than what was actually created/deleted.
 * <p>
 * Each test below drives {@code database.command("redis", ...)} directly, the same entry point
 * {@code RedisQueryLanguageTest.queryEngineDirectly()} uses, rather than going through the HTTP command endpoint:
 * {@code DatabaseAbstractHandler.executeInTransaction} wraps every HTTP command in its own outer
 * {@code database.transaction(block, false, retries)} first, which makes {@code hSet()}'s/{@code
 * executeTransaction()}'s own transaction join an already-active one ({@code createdNewTx = false}) rather than be
 * the outermost - so retrying happens at the HTTP wrapper, which re-invokes the Redis command from scratch on each
 * attempt with a brand new accumulator, and the bug this issue describes is not reachable through it. Called
 * directly, with nothing else on the thread holding a transaction open, {@code hSet()}'s/{@code
 * executeTransaction()}'s own {@code database.transaction(...)} call is the genuinely outermost one and its retry
 * loop is live - the same condition the issue's own "Where" section describes and that the RESP wire protocol
 * (issue #6560's {@code RedisNetworkExecutor}) reaches for HSET/HDEL, though it has no MULTI/EXEC of its own.
 * <p>
 * Each test forces exactly one retry deterministically with a {@code BeforeRecordCreateListener} /
 * {@code BeforeRecordDeleteListener} that throws {@link ConcurrentModificationException} once, on the third record
 * of the first attempt - the same repro the issue itself was confirmed with.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8037RedisRetryReplyDuplicationTest extends BaseRedisServerTest {

  /**
   * Throws a {@link ConcurrentModificationException} the first time {@code seen} reaches {@code failAt}, and lets
   * every event after that - including the retry - through.
   */
  private static BeforeRecordCreateListener failNthCreate(final AtomicInteger seen, final int failAt) {
    return record -> {
      if (seen.incrementAndGet() == failAt)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #8037 repro)");
      return true;
    };
  }

  private static BeforeRecordDeleteListener failNthDelete(final AtomicInteger seen, final int failAt) {
    return record -> {
      if (seen.incrementAndGet() == failAt)
        throw new ConcurrentModificationException("forced MVCC conflict (issue #8037 repro)");
      return true;
    };
  }

  @Test
  void multiExecAnswersExactlyAsManyRepliesAsCommandsEvenAfterARetry() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordCreateListener listener = failNthCreate(seen, 3);
    database.getSchema().getType("Person").getEvents().registerListener(listener);
    try {
      final String transaction = """
          MULTI
          HSET Person {"id":1}
          HSET Person {"id":2}
          HSET Person {"id":3}
          EXEC
          """;

      try (final ResultSet rs = database.command("redis", transaction)) {
        final Result result = rs.next();
        final Object value = result.getProperty("value");
        assertThat(value).isInstanceOf(List.class);
        final List<?> results = (List<?>) value;
        assertThat(results.size())
            .as("an EXEC of 3 commands must answer exactly 3 replies, not the rolled-back attempt's counted twice")
            .isEqualTo(3);
      }
    } finally {
      database.getSchema().getType("Person").getEvents().unregisterListener(listener);
    }

    assertThat(countOf(database, "Person"))
        .as("the retried attempt must still have committed exactly 3 documents")
        .isEqualTo(3);
  }

  @Test
  void hSetAnswersExactlyTheDocumentsItCreatedEvenAfterARetry() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.id INTEGER");
    database.command("sql", "CREATE INDEX ON Item (id) UNIQUE");

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordCreateListener listener = failNthCreate(seen, 3);
    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try (final ResultSet rs = database.command("redis", "HSET Item {\"id\":1} {\"id\":2} {\"id\":3}")) {
      final Object value = rs.next().getProperty("value");
      assertThat(((Number) value).intValue())
          .as("HSET of 3 documents must report 3 even after one MVCC retry, not the discarded attempt's count too")
          .isEqualTo(3);
    } finally {
      database.getSchema().getType("Item").getEvents().unregisterListener(listener);
    }

    assertThat(countOf(database, "Item")).isEqualTo(3);
  }

  @Test
  void hDelAnswersExactlyTheDocumentsItDeletedEvenAfterARetry() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Widget");
    database.command("sql", "CREATE PROPERTY Widget.id INTEGER");
    database.command("sql", "CREATE INDEX ON Widget (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("Widget").set("id", i).save();
    });

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordDeleteListener listener = failNthDelete(seen, 3);
    database.getSchema().getType("Widget").getEvents().registerListener(listener);
    try (final ResultSet rs = database.command("redis", "HDEL Widget[id] 1 2 3")) {
      final Object value = rs.next().getProperty("value");
      assertThat(((Number) value).intValue())
          .as("HDEL of 3 keys must report 3 even after one MVCC retry, not the discarded attempt's count too")
          .isEqualTo(3);
    } finally {
      database.getSchema().getType("Widget").getEvents().unregisterListener(listener);
    }

    assertThat(countOf(database, "Widget")).isZero();
  }

  /**
   * Found while fixing the accumulator bug above: {@code executeRedisCommand()} caught every exception that
   * escaped a command - including a {@link ConcurrentModificationException} a write command's own {@code
   * database.transaction(...)} is set up to retry - and wrapped it into {@code CommandParsingException} before it
   * could reach either retry loop. Driven over the HTTP command endpoint specifically, because that is where the
   * wrapping mattered most: {@code DatabaseAbstractHandler.executeInTransaction} wraps the whole call in its own
   * outer {@code database.transaction(block, false, retries)}, and that outer retry loop is the one whose {@code
   * catch (NeedRetryException | DuplicatedKeyException)} the wrapped exception type used to defeat, turning a
   * conflict a second attempt would have committed into a hard, unretried failure on the first.
   */
  @Test
  void anMvccConflictOverTheCommandEndpointIsRetriedInsteadOfFailingHard() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Gadget");
    database.command("sql", "CREATE PROPERTY Gadget.id INTEGER");
    database.command("sql", "CREATE INDEX ON Gadget (id) UNIQUE");

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordCreateListener listener = failNthCreate(seen, 1);
    database.getSchema().getType("Gadget").getEvents().registerListener(listener);
    try {
      final JSONObject response = executeCommand(0, "redis", "HSET Gadget {\"id\":1}");
      assertThat(getResultValueAsInt(response))
          .as("the conflict on the first attempt must be retried by the HTTP auto-commit wrapper, not answered "
              + "as a hard failure")
          .isEqualTo(1);
    } finally {
      database.getSchema().getType("Gadget").getEvents().unregisterListener(listener);
    }

    assertThat(countOf(database, "Gadget")).isEqualTo(1);
  }

  private long countOf(final Database database, final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  /** Extracts the "value" property from the first result in the response: {"result": [{"value": <result>}]}. */
  private Object getResultValue(final JSONObject response) {
    final JSONArray results = response.getJSONArray("result");
    if (results.isEmpty())
      return null;
    final JSONObject firstResult = results.getJSONObject(0);
    if (firstResult.isNull("value"))
      return null;
    return firstResult.get("value");
  }

  private int getResultValueAsInt(final JSONObject response) {
    final Object value = getResultValue(response);
    if (value instanceof Number number)
      return number.intValue();
    return Integer.parseInt(value.toString());
  }

  /**
   * Not a drop-in duplicate of {@link BaseGraphServerTest#executeCommand}: that one hardcodes the "studio"
   * serializer, which wraps the reply as {@code result: {...}} (an object) rather than the raw
   * {@code result: [...]} array shape {@link #getResultValue} parses below and the actual redis client/HTTP
   * caller this test is reproducing gets - checked while addressing a review suggestion to reuse the base
   * method, which changes {@code getResultValue} into a runtime type-error rather than compiling to something
   * silently wrong. Kept separate on purpose; the only thing this repeats from the base method is asking the
   * server for its actual bound port (issue #6560) rather than assuming 2480+serverIndex.
   */
  @Override
  protected JSONObject executeCommand(final int serverIndex, final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject();
    request.put("language", language);
    request.put("command", command);

    try (OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }

    final int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      final String error = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException("HTTP " + responseCode + ": " + error);
    }

    final String response = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new JSONObject(response);
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
