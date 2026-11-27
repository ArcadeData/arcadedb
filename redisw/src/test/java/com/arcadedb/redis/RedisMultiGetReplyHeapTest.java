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
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The reply of HMGET has one entry per key of the request, so its size follows the request and not only the data. The heap it takes
 * is accounted to the budget the queries of the JVM share, on the wire path and on the {@code redis} query language, and a reply that
 * does not fit is answered with an error instead of being built.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class RedisMultiGetReplyHeapTest extends BaseRedisServerTest {
  private static final String PASSWORD      = DEFAULT_PASSWORD_FOR_TESTS;
  private static final long   MB            = 1024 * 1024;
  private static final int    PAYLOAD_CHARS = 20_000;
  private static final int    RECORDS       = 200;
  private static final String HEAVY_INDEX   = "HeavyItem[id]";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  void restoreBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aReplyThatCannotFitTheBudgetIsRefused() throws Exception {
    final String[] rids = createHeavyRecords();
    final long baseline = QueryHeapBudget.getReservedBytes();
    // 8MB of budget past what is held now: the 200 entries of 20KB are beyond it
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 8);

    // the kind of refusal depends on whether other queries hold the budget at that moment (TRYAGAIN), or not (ERR)
    final String refusal = "(?s)(ERR|TRYAGAIN) .*";
    try (final Jedis jedis = newClient()) {
      assertThatThrownBy(() -> jedis.hmget(getDatabaseName(), rids)).isInstanceOf(JedisDataException.class).hasMessageMatching(refusal);
      assertThatThrownBy(() -> jedis.hmget(getDatabaseName() + "." + HEAVY_INDEX, ids(RECORDS))).isInstanceOf(
          JedisDataException.class).hasMessageMatching(refusal);

      // the transient form reads a stored value for every key just the same
      for (int i = 0; i < RECORDS; i++)
        jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(),
            "{\"id\":\"v" + i + "\",\"payload\":\"" + "y".repeat(PAYLOAD_CHARS) + "\"}");
      assertThatThrownBy(() -> jedis.hmget(getDatabaseName(), variables(RECORDS))).isInstanceOf(JedisDataException.class)
          .hasMessageMatching(refusal);

      // each refusal was one whole reply: the connection is still in step, and a reply that fits is served
      assertThat(jedis.ping()).isEqualTo("PONG");
      assertThat(jedis.hmget(getDatabaseName(), Arrays.copyOf(rids, 10))).hasSize(10);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).as("the replies gave their heap back").isLessThanOrEqualTo(baseline);
  }

  @Test
  void aReplyRefusedBecauseOtherQueriesHoldTheBudgetCanBeRetried() throws Exception {
    final String[] rids = Arrays.copyOf(createHeavyRecords(), 100);
    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 64);

    // other queries hold all the budget but 1MB
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - MB,
        "other queries");
    try (final Jedis jedis = newClient()) {
      try {
        assertThatThrownBy(() -> jedis.hmget(getDatabaseName(), rids)).isInstanceOf(JedisDataException.class)
            .hasMessageStartingWith("TRYAGAIN");
        assertThat(jedis.ping()).isEqualTo("PONG");
      } finally {
        others.close();
      }

      final List<String> served = jedis.hmget(getDatabaseName(), rids);
      assertThat(served).hasSize(100);
      for (int i = 0; i < served.size(); i++)
        assertThat(idOf(served.get(i))).isEqualTo(i);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).as("the served reply gave its heap back").isLessThanOrEqualTo(baseline);
  }

  @Test
  void everyKeyKeepsItsPlaceInTheReplyInEveryForm() throws Exception {
    final String[] rids = createHeavyRecords();
    final String missingRid = rids[0].substring(0, rids[0].indexOf(':') + 1) + "99999";

    try (final Jedis jedis = newClient()) {
      final List<String> byRid = jedis.hmget(getDatabaseName(), rids[3], missingRid, rids[1]);
      assertThat(byRid).hasSize(3);
      assertThat(idOf(byRid.get(0))).isEqualTo(3);
      assertThat(byRid.get(1)).isNull();
      assertThat(idOf(byRid.get(2))).isEqualTo(1);

      final List<String> byIndex = jedis.hmget(getDatabaseName() + "." + HEAVY_INDEX, "2", "9999", "0");
      assertThat(byIndex).hasSize(3);
      assertThat(idOf(byIndex.get(0))).isEqualTo(2);
      assertThat(byIndex.get(1)).isNull();
      assertThat(idOf(byIndex.get(2))).isEqualTo(0);

      jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "{\"id\":\"k1\",\"n\":1}");
      jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "{\"id\":\"k2\",\"n\":2}");
      final List<String> byVariable = jedis.hmget(getDatabaseName(), "k2", "nope", "k1", rids[3]);
      assertThat(byVariable).hasSize(4);
      assertThat(byVariable.get(0)).contains("\"n\":2");
      assertThat(byVariable.get(1)).isNull();
      assertThat(byVariable.get(2)).contains("\"n\":1");
      assertThat(byVariable.get(3)).isEqualTo(byRid.get(0));
    }
  }

  @Test
  void aKeyThatFailsAfterSomeEntriesWereWrittenLeavesTheErrorAsTheOnlyReply() throws Exception {
    final String[] rids = createHeavyRecords();

    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final String auth = "*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$" + PASSWORD.length() + "\r\n" + PASSWORD + "\r\n";
      // two entries are read before the third key, which is not a RID, fails the command
      final String bogus = "#not-a-rid";
      final String hmget = "*5\r\n$5\r\nHMGET\r\n$" + getDatabaseName().length() + "\r\n" + getDatabaseName() + "\r\n$" + rids[0].length()
          + "\r\n" + rids[0] + "\r\n$" + rids[1].length() + "\r\n" + rids[1] + "\r\n$" + bogus.length() + "\r\n" + bogus + "\r\n";
      socket.getOutputStream().write((auth + hmget + "*1\r\n$4\r\nPING\r\n").getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();

      assertThat(line(socket)).isEqualTo("+OK");
      assertThat(line(socket)).startsWith("-");
      assertThat(line(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void theQueryLanguageAccountsTheEntriesOfItsAnswerToo() throws Exception {
    final String[] rids = createHeavyRecords();
    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 64);

    final JSONArray served = (JSONArray) getResultValue(executeCommand(0, "redis", "HMGET " + String.join(" ", Arrays.copyOf(rids, 3))));
    assertThat(served.length()).isEqualTo(3);

    final JSONArray byIndex = (JSONArray) getResultValue(executeCommand(0, "redis", "HMGET " + HEAVY_INDEX + " 0 9999 1"));
    assertThat(byIndex.length()).isEqualTo(3);
    assertThat(byIndex.isNull(1)).isTrue();

    // other queries hold all the budget but 1MB: a hundred entries of 20KB do not fit what is left
    final String ridsCommand = "HMGET " + String.join(" ", Arrays.copyOf(rids, 100));
    final String indexCommand = "HMGET " + HEAVY_INDEX + " " + String.join(" ", ids(100));
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - MB,
        "other queries");
    try {
      assertThatThrownBy(() -> executeCommand(0, "redis", ridsCommand)).hasMessageStartingWith("HTTP 503");
      assertThatThrownBy(() -> executeCommand(0, "redis", indexCommand)).hasMessageStartingWith("HTTP 503");
    } finally {
      others.close();
    }

    final JSONObject again = executeCommand(0, "redis", ridsCommand);
    assertThat(((JSONArray) getResultValue(again)).length()).isEqualTo(100);
    assertThat(QueryHeapBudget.getReservedBytes()).as("the answers gave their heap back").isLessThanOrEqualTo(baseline);
  }

  @Test
  void theAnswersOfABatchAreAccountedTogetherAndGivenBackWithTheResultSet() throws Exception {
    final String[] rids = createHeavyRecords();
    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 64);

    // four commands of thirty entries: each takes about 2.4MB of accounting, so each fits what is left on its own, and the four do not
    final StringBuilder batch = new StringBuilder();
    for (int line = 0; line < 4; line++)
      batch.append("HMGET ").append(String.join(" ", Arrays.copyOfRange(rids, line * 30, line * 30 + 30))).append('\n');

    // other queries hold all the budget but 5MB
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - 5 * MB,
        "other queries");
    try {
      assertThatThrownBy(() -> executeCommand(0, "redis", batch.toString())).hasMessageStartingWith("HTTP 503");
    } finally {
      others.close();
    }

    final JSONArray answers = (JSONArray) getResultValue(executeCommand(0, "redis", batch.toString()));
    assertThat(answers.length()).isEqualTo(4);
    assertThat(answers.getJSONArray(3).length()).isEqualTo(30);
    assertThat(QueryHeapBudget.getReservedBytes()).as("the result set gave the heap of its answers back").isLessThanOrEqualTo(baseline);
  }

  /** {@link #RECORDS} records of about {@link #PAYLOAD_CHARS} characters, indexed by {@code id}, the RIDs in the order of the ids. */
  private String[] createHeavyRecords() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        CREATE DOCUMENT TYPE HeavyItem;\
        CREATE PROPERTY HeavyItem.id INTEGER;\
        CREATE INDEX ON HeavyItem (id) UNIQUE;""");
    final String payload = "x".repeat(PAYLOAD_CHARS);
    final String[] rids = new String[RECORDS];
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument("HeavyItem").set("id", i, "payload", payload).save().getIdentity().toString();
    });
    return rids;
  }

  private Jedis newClient() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", PASSWORD);
    return jedis;
  }

  private static int idOf(final String json) {
    return new JSONObject(json).getInt("id");
  }

  private static String[] ids(final int count) {
    return IntStream.range(0, count).mapToObj(String::valueOf).toArray(String[]::new);
  }

  private static String[] variables(final int count) {
    return IntStream.range(0, count).mapToObj(i -> "v" + i).toArray(String[]::new);
  }

  private static String line(final Socket socket) throws IOException {
    final InputStream in = socket.getInputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    int c;
    while ((c = in.read()) != -1) {
      if (c == '\n')
        break;
      if (c != '\r')
        out.write(c);
    }
    return out.toString(StandardCharsets.UTF_8);
  }
}
