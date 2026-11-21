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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12797: a script groups the statements of a top-level {@code BEGIN} up to its {@code COMMIT} into one block, and
 * dropped every statement of a block that no {@code COMMIT} closed: a {@code BEGIN ... ROLLBACK} script and the statements after
 * its {@code ROLLBACK} ran nothing, and a forgotten {@code COMMIT} lost the writes without a word.
 */
class Issue12797ScriptUnclosedBeginTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
  }

  private List<Integer> values() {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T ORDER BY v")) {
      while (rs.hasNext())
        values.add(rs.next().<Integer>getProperty("v"));
    }
    return values;
  }

  private Object lastValue(final String script) {
    Object last = null;
    try (final ResultSet rs = database.command("sqlscript", script)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        last = row.getProperty("value");
      }
    }
    return last;
  }

  @Test
  void statementsAfterAScriptRollbackRun() {
    assertThat(lastValue("BEGIN;\nINSERT INTO T SET v = 1;\nROLLBACK;\nRETURN 7;")).isEqualTo(7);
    assertThat(values()).isEmpty();
  }

  @Test
  void rollbackThenAnotherCommittedBlock() {
    assertThat(lastValue("BEGIN;\nINSERT INTO T SET v = 1;\nROLLBACK;\nBEGIN;\nINSERT INTO T SET v = 2;\nCOMMIT;\nRETURN 1;")).isEqualTo(1);
    assertThat(values()).containsExactly(2);
  }

  @Test
  void commitedBlockThenRolledBackBlockThenReturn() {
    assertThat(lastValue("BEGIN;\nINSERT INTO T SET v = 1;\nCOMMIT;\nBEGIN;\nINSERT INTO T SET v = 2;\nROLLBACK;\nRETURN 'done';"))
        .isEqualTo("done");
    assertThat(values()).containsExactly(1);
  }

  @Test
  void queryInsideTheRolledBackBlockSeesItsWrites() {
    try (final ResultSet rs = database.command("sqlscript",
        "BEGIN;\nINSERT INTO T SET v = 1;\nLET $c = SELECT count(*) AS c FROM T;\nROLLBACK;\nRETURN $c[0].c;")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("value").longValue()).isEqualTo(1L);
    }
    assertThat(values()).isEmpty();
  }

  @Test
  void aBeginNoCommitOrRollbackClosesIsRefused() {
    assertThatThrownBy(() -> database.command("sqlscript", "BEGIN;\nINSERT INTO T SET v = 1;"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("BEGIN");
    assertThatThrownBy(() -> database.command("sqlscript", "BEGIN;\nINSERT INTO T SET v = 1;\nBEGIN;\nINSERT INTO T SET v = 2;\nCOMMIT;"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("BEGIN");
    assertThat(values()).isEmpty();
  }

  @Test
  void commitBlocksStillRun() {
    assertThat(lastValue("BEGIN;\nINSERT INTO T SET v = 1;\nCOMMIT;\nRETURN 5;")).isEqualTo(5);
    assertThat(lastValue("BEGIN;\nINSERT INTO T SET v = 2;\nCOMMIT RETRY 2;\nRETURN 6;")).isEqualTo(6);
    assertThat(values()).containsExactly(1, 2);
  }
}
