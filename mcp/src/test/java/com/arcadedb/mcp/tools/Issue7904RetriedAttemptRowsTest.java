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
package com.arcadedb.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7904: {@code Database.transaction(TransactionScope)} re-runs its block up to {@code arcadedb.txRetries}
 * times when an attempt loses an MVCC race, rolling the failed attempt back first. Three MCP tools built their
 * reply into a {@code JSONArray} declared OUTSIDE that block and never cleared it, so a retried attempt APPENDED
 * to what the rolled-back one had already put there. The reply then named the same record once per attempt, its
 * {@code count} was the inflated length, and the surplus rows carried the values of the attempt that was rolled
 * back - a record state that is in no database, and the one error an agent reading the reply cannot detect.
 * <p>
 * {@code execute_command} had a fourth: its {@code limit} was counted in a local of the block, which restarts at
 * 0 on every attempt while the rows it caps did not, so two attempts could return twice the advertised bound.
 * <p>
 * Each test forces exactly one retry, deterministically: an {@link AfterRecordUpdateListener} fires while the
 * FIRST attempt holds the page, runs a conflicting commit on another thread and waits for it, so the attempt's
 * own commit is refused and the engine replays the block.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7904RetriedAttemptRowsTest extends BaseGraphServerTest {
  private static final String TYPE_NAME = "Issue7904Counter";

  private MCPConfiguration          config;
  private ServerSecurityUser        user;
  private DatabaseInternal          embedded;
  private AfterRecordUpdateListener conflictOnFirstAttempt;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowSchemaChange(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    embedded = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName()).getEmbedded();
    // The unique index on the match key is what upsert_entity's own description asks the operator for.
    final VertexType type = embedded.getSchema().getOrCreateVertexType(TYPE_NAME);
    type.getOrCreateProperty("k", Type.INTEGER);
    type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
  }

  @AfterEach
  void cleanup() {
    if (conflictOnFirstAttempt != null)
      embedded.getEvents().unregisterListener(conflictOnFirstAttempt);
    if (embedded != null && embedded.getSchema().existsType(TYPE_NAME))
      embedded.getSchema().dropType(TYPE_NAME);
  }

  @Test
  void executeCommandReturnsOnlyTheCommittedAttemptsRows() {
    embedded.transaction(() -> embedded.command("sql", "INSERT INTO " + TYPE_NAME + " SET k = 1, n = 0"));

    final AtomicInteger attempts = conflictOnTheFirstAttempt("UPDATE " + TYPE_NAME + " SET n = 99 WHERE k = 1");

    final JSONObject result = ExecuteCommandTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "UPDATE " + TYPE_NAME + " SET n = n + 1 RETURN AFTER @this WHERE k = 1"), config);

    assertThat(attempts.get()).as("the conflicting writer must have forced exactly one replay").isEqualTo(1);

    final JSONArray records = result.getJSONArray("records");
    assertThat(records.length()).as("one vertex updated once is one row, not one row per attempt").isEqualTo(1);
    assertThat(result.getInt("count")).isEqualTo(1);
    // The rolled-back attempt computed n = 0 + 1; the committed one replayed on the conflicting writer's 99.
    assertThat(records.getJSONObject(0).getInt("n")).as("the row must be the committed state").isEqualTo(100);
    assertThat(readCommittedN()).isEqualTo(100);
  }

  @Test
  void executeCommandKeepsItsLimitAcrossARetry() {
    embedded.transaction(() -> {
      for (int i = 1; i <= 4; i++)
        embedded.command("sql", "INSERT INTO " + TYPE_NAME + " SET k = " + i + ", n = 0");
    });

    final AtomicInteger attempts = conflictOnTheFirstAttempt("UPDATE " + TYPE_NAME + " SET n = 99 WHERE k = 1");

    final JSONObject result = ExecuteCommandTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "UPDATE " + TYPE_NAME + " SET n = n + 1 RETURN AFTER @this")
        .put("limit", 2), config);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(result.getJSONArray("records").length())
        .as("'limit' bounds what the tool RETURNS, not what one attempt collected").isEqualTo(2);
    assertThat(result.getInt("count")).isEqualTo(2);
  }

  @Test
  void upsertEntityReportsTheOneVertexItTouched() {
    embedded.transaction(() -> embedded.command("sql", "INSERT INTO " + TYPE_NAME + " SET k = 1, n = 0"));

    final AtomicInteger attempts = conflictOnTheFirstAttempt("UPDATE " + TYPE_NAME + " SET n = 99 WHERE k = 1");

    final JSONObject result = UpsertEntityTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", TYPE_NAME)
        .put("matchKeys", new JSONObject().put("k", 1))
        .put("setProperties", new JSONObject().put("tag", "upserted")), config);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(result.getJSONArray("records").length())
        .as("a MERGE on one match key touches one vertex, whatever the engine had to replay").isEqualTo(1);
    assertThat(result.getInt("count")).isEqualTo(1);
    assertThat(readCommittedN()).as("the surviving state is the conflicting writer's, then the replay's")
        .isEqualTo(99);
  }

  /**
   * Registers a listener that, the first time an update runs on the calling thread, commits {@code command} from
   * another thread and waits for it: the attempt in flight then holds a page the committed writer has already
   * moved on, so its own commit is refused with a {@code ConcurrentModificationException} and the engine replays
   * the whole block. Returns the counter of forced replays.
   */
  private AtomicInteger conflictOnTheFirstAttempt(final String command) {
    final AtomicInteger forced = new AtomicInteger();
    final Thread owner = Thread.currentThread();
    conflictOnFirstAttempt = new AfterRecordUpdateListener() {
      @Override
      public void onAfterUpdate(final Record record) {
        // Only from the attempt under test, and only once: the conflicting writer updates too.
        if (Thread.currentThread() != owner || forced.get() > 0)
          return;
        forced.incrementAndGet();

        final Thread conflicting = new Thread(() -> {
          final Database db = embedded;
          db.transaction(() -> db.command("sql", command));
        }, "issue7904-conflicting-writer");
        conflicting.start();
        try {
          conflicting.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    embedded.getEvents().registerListener(conflictOnFirstAttempt);
    return forced;
  }

  private int readCommittedN() {
    return embedded.query("sql", "SELECT n FROM " + TYPE_NAME + " WHERE k = 1").next().getProperty("n");
  }
}
