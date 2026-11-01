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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests for #11114: {@code apoc.util.validate} took two arguments where APOC defines three
 * ({@code predicate, message, params}), and it raised when the predicate was FALSE, where APOC raises when it is TRUE
 * (<a href="https://neo4j.com/docs/apoc/current/overview/apoc.util/apoc.util.validate/">APOC reference</a>), so a guard
 * ported from Neo4j aborted exactly the writes it was written to let through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11114ApocUtilValidateTest extends TestHelper {

  @Test
  void aFalsePredicateLetsTheQueryThrough() {
    assertThat(rows("CALL apoc.util.validate(false, 'bad %s', ['x']) RETURN 1 AS ok")).hasSize(1);
    assertThat(rows("CALL apoc.util.validate(false, 'bad') RETURN 1 AS ok")).hasSize(1);
    assertThat(rows("CALL util.validate(false, 'bad %s', ['x']) RETURN 1 AS ok")).hasSize(1);
  }

  @Test
  void aTruePredicateRaisesTheFormattedMessage() {
    final Throwable error = catchThrowable(() -> rows("CALL apoc.util.validate(true, 'bad %s', ['x']) RETURN 1 AS ok"));
    assertThat(error).isNotNull();
    assertThat(rootMessage(error)).isEqualTo("bad x");
    // a driver shows the outermost message only, so the text the query aborted with must be in it
    assertThat(error.getMessage()).contains("bad x");
    // the caller's condition, not a server fault
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void severalPlaceholdersAndNonStringParameters() {
    final Throwable error = catchThrowable(
        () -> rows("CALL apoc.util.validate(1 < 2, 'node %s not found in %s (%s)', [42, 'Person', true]) RETURN 1 AS ok"));
    assertThat(rootMessage(error)).isEqualTo("node 42 not found in Person (true)");
  }

  @Test
  void withoutParametersTheMessageIsTakenAsItIs() {
    // APOC formats only when params are given, so a literal % in a plain message stays as written
    assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '100% wrong') RETURN 1 AS ok"))))
        .isEqualTo("100% wrong");
    assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '100% wrong', []) RETURN 1 AS ok"))))
        .isEqualTo("100% wrong");
  }

  @Test
  void aNullPredicateDoesNotRaise() {
    assertThat(rows("CALL apoc.util.validate(null, 'bad', []) RETURN 1 AS ok")).hasSize(1);
  }

  @Test
  void theGuardAbortsOnlyTheWriteItProtects() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Account {id: 1, balance: 10})"));

    // the usual "not found" guard: the account exists, so the write goes through
    database.transaction(() -> database.command("opencypher", """
        MATCH (a:Account {id: $id})
        CALL apoc.util.validate(a.balance < $amount, 'insufficient balance on %s', [$id])
        SET a.balance = a.balance - $amount""", Map.of("id", 1, "amount", 4)));
    assertThat(rows("MATCH (a:Account {id: 1}) RETURN a.balance AS b").getFirst().<Number>getProperty("b").intValue())
        .isEqualTo(6);

    final Throwable error = catchThrowable(() -> database.transaction(() -> database.command("opencypher", """
        MATCH (a:Account {id: $id})
        CALL apoc.util.validate(a.balance < $amount, 'insufficient balance on %s', [$id])
        SET a.balance = a.balance - $amount""", Map.of("id", 1, "amount", 100))));
    assertThat(rootMessage(error)).isEqualTo("insufficient balance on 1");
    assertThat(rows("MATCH (a:Account {id: 1}) RETURN a.balance AS b").getFirst().<Number>getProperty("b").intValue())
        .isEqualTo(6);
  }

  @Test
  void validatePredicateIsTheFunctionForm() {
    assertThat(rows("RETURN apoc.util.validatePredicate(false, 'bad %s', ['x']) AS ok").getFirst().<Boolean>getProperty("ok"))
        .isTrue();
    assertThat(rootMessage(catchThrowable(() -> rows("RETURN apoc.util.validatePredicate(true, 'bad %s', ['x']) AS ok"))))
        .isEqualTo("bad x");
  }

  @Test
  void aWrongArgumentCountIsStillRefused() {
    assertThat(catchThrowable(() -> rows("CALL apoc.util.validate(true) RETURN 1 AS ok"))).isNotNull();
    assertThat(catchThrowable(() -> rows("CALL apoc.util.validate(true, 'a', [], 'extra') RETURN 1 AS ok"))).isNotNull();
  }

  private List<Result> rows(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }

  private static String rootMessage(final Throwable error) {
    assertThat(error).isNotNull();
    Throwable t = error;
    while (t.getCause() != null && t.getCause() != t)
      t = t.getCause();
    return t.getMessage();
  }
}
