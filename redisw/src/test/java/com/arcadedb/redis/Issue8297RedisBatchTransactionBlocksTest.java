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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8297: a {@code redis}-language batch returned from the middle of its loop at the first EXEC or DISCARD, so
 * every line after it was silently dropped while the reply was a normal success. Commands before MULTI were queued
 * into the transaction EXEC opened, and an EXEC with no MULTI committed the whole prefix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8297RedisBatchTransactionBlocksTest extends BaseRedisServerTest {

  @Test
  void linesAfterExecAreExecuted() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(replies(database, "MULTI\nSET i8297a 1\nEXEC\nSET i8297b 2\nSET i8297c 3")).containsExactly("OK", "OK", "OK");

    assertThat(get(database, "i8297a")).isEqualTo("1");
    assertThat(get(database, "i8297b")).isEqualTo("2");
    assertThat(get(database, "i8297c")).isEqualTo("3");
  }

  @Test
  void linesAfterDiscardAreExecuted() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(replies(database, "MULTI\nSET i8297d 4\nDISCARD\nSET i8297e 5")).containsExactly("OK");

    assertThat(get(database, "i8297d")).isNull();
    assertThat(get(database, "i8297e")).isEqualTo("5");
  }

  @Test
  void onlyADiscardedBlockAnswersOk() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final ResultSet rs = database.command("redis", "MULTI\nSET i8297f 6\nDISCARD")) {
      assertThat((Object) rs.next().getProperty("value")).isEqualTo("OK");
    }
    assertThat(get(database, "i8297f")).isNull();
  }

  @Test
  void multipleBlocksAndLooseCommandsAnswerOneReplyPerExecutedCommandInOrder() {
    final Database database = getServerDatabase(0, getDatabaseName());

    final List<Object> replies = replies(database,
        "SET i8297g 10\nMULTI\nINCR i8297g\nSET i8297h x\nEXEC\nGET i8297h\nMULTI\nSET i8297i y\nDISCARD\nMULTI\nINCR i8297g\nEXEC\nGET i8297g");
    assertThat(replies).hasSize(6);
    assertThat(replies.get(0)).isEqualTo("OK");
    assertThat(((Number) replies.get(1)).longValue()).isEqualTo(11L);
    assertThat(replies.get(2)).isEqualTo("OK");
    assertThat(replies.get(3)).isEqualTo("x");
    assertThat(((Number) replies.get(4)).longValue()).isEqualTo(12L);
    assertThat(((Number) replies.get(5)).longValue()).isEqualTo(12L);
    assertThat(get(database, "i8297i")).isNull();
  }

  /**
   * A command before MULTI runs on its own, so a block that fails and rolls back does not take it with it.
   */
  @Test
  void commandBeforeMultiIsNotPartOfTheTransaction() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE I8297Doc");
    database.command("sql", "CREATE PROPERTY I8297Doc.id INTEGER");
    database.command("sql", "CREATE INDEX ON I8297Doc (id) UNIQUE");

    assertThatThrownBy(() -> database.command("redis", """
        HSET I8297Doc {"id":1}
        MULTI
        HSET I8297Doc {"id":2}
        NOSUCHCOMMAND
        EXEC""").close()).isInstanceOf(CommandParsingException.class);

    try (final ResultSet rs = database.query("sql", "SELECT id FROM I8297Doc ORDER BY id")) {
      assertThat(rs.stream().map(r -> ((Number) r.getProperty("id")).intValue()).toList()).containsExactly(1);
    }
  }

  @Test
  void malformedBatchesAreRefusedBeforeAnyCommandRuns() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThatThrownBy(() -> database.command("redis", "SET i8297r 9\nEXEC").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("EXEC without MULTI");
    assertThatThrownBy(() -> database.command("redis", "SET i8297r 9\nDISCARD").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("DISCARD without MULTI");
    assertThatThrownBy(() -> database.command("redis", "SET i8297r 9\nMULTI\nMULTI\nEXEC").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("nested");
    assertThatThrownBy(() -> database.command("redis", "SET i8297r 9\nMULTI\nSET i8297s 1").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("MULTI without EXEC");

    assertThat(get(database, "i8297r")).isNull();
    assertThat(get(database, "i8297s")).isNull();
  }

  private static List<Object> replies(final Database database, final String batch) {
    try (final ResultSet rs = database.command("redis", batch)) {
      return rs.next().getProperty("value");
    }
  }

  private static Object get(final Database database, final String key) {
    try (final ResultSet rs = database.command("redis", "GET " + key)) {
      return rs.hasNext() ? rs.next().getProperty("value") : null;
    }
  }
}
