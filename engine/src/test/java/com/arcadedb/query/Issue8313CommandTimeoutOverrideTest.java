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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8313: a write a follower forwards to the leader waits for the {@code arcadedb.command.timeout} budget the
 * follower resolved, and the leader used to enforce whatever its OWN database configuration held. The budget now
 * travels with the forward and the leader publishes it through {@link CommandTimeoutOverride}, which every command
 * context reads at its root in place of the database setting. These cases pin the engine half: the published budget
 * is the one a command runs under, whatever the language, and nothing of it outlives {@link CommandTimeoutOverride#clear()}.
 * <p>
 * The self-join below walks {@code NODES^2} pairs, orders of magnitude more than the 50 ms budget it is given; the
 * assertion is on the setting the abort names, not on elapsed time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8313CommandTimeoutOverrideTest {
  private static final int    NODES   = 3_000;
  private static final String DB_PATH = "./target/databases/test-issue-8313-command-timeout-override";

  private static Database database;

  @BeforeAll
  static void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.transaction(() -> {
      for (int i = 0; i < NODES; i++)
        database.newVertex("Node").set("v", i).save();
    });
  }

  @AfterAll
  static void teardown() {
    if (database != null)
      database.drop();
  }

  @AfterEach
  void clear() {
    CommandTimeoutOverride.clear();
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @Test
  void thePublishedBudgetTakesThePlaceOfTheDatabaseSetting() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);
    CommandTimeoutOverride.set(1_234L);

    assertThat(new BasicCommandContext().setDatabase((DatabaseInternal) database).getCommandTimeout()).isEqualTo(1_234L);
    assertThat(CommandTimeoutOverride.effectiveTimeout((DatabaseInternal) database)).isEqualTo(1_234L);

    CommandTimeoutOverride.clear();
    assertThat(new BasicCommandContext().setDatabase((DatabaseInternal) database).getCommandTimeout())
        .as("once cleared, the database's own setting applies again")
        .isEqualTo(60_000L);
  }

  @Test
  void aSqlCommandRunsUnderThePublishedBudgetWhenTheDatabaseHasNone() {
    CommandTimeoutOverride.set(50L);

    assertThatThrownBy(() -> drain("sql",
        "MATCH {type: Node, as: a}, {type: Node, as: b, where: (v + $matched.a.v = -1)} RETURN a.v, b.v"))
        .as("the leader has no budget of its own, and must still enforce the one the follower waits for")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 50ms");
  }

  @Test
  void anOpenCypherCommandRunsUnderThePublishedBudgetToo() {
    CommandTimeoutOverride.set(50L);

    assertThatThrownBy(() -> drain("opencypher", "MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN count(*) AS c"))
        .as("the budget is read where every command context resolves it, not by one language only")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 50ms");
  }

  @Test
  void onlyAPositiveBudgetIsAccepted() {
    assertThat(CommandTimeoutOverride.parse("250")).isEqualTo(250L);
    assertThat(CommandTimeoutOverride.parse(" 250 ")).isEqualTo(250L);
    assertThat(CommandTimeoutOverride.parse("0")).isEqualTo(-1L);
    assertThat(CommandTimeoutOverride.parse("-5")).isEqualTo(-1L);
    assertThat(CommandTimeoutOverride.parse("soon")).isEqualTo(-1L);
    assertThat(CommandTimeoutOverride.parse(null)).isEqualTo(-1L);

    CommandTimeoutOverride.set(0L);
    assertThat(CommandTimeoutOverride.get()).as("0 is 'unbounded' for the setting, never a budget to publish").isEqualTo(-1L);
  }

  private static void drain(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
