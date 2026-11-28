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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.NamedParameter;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.PositionalParameter;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A RID written as {@code #bucket:position} is a leaf of the walks of {@link SqlAstInspector} (issue #13269): modelhub scores a
 * batch of up to 5,000 chunks with {@code SELECT ... FROM [#b:p, ...]}, a statement too big for the statement cache that is parsed
 * afresh by every execution and then walked twice (its placeholders, the statements it nests), fifteen thousand nodes a walk. A RID
 * given as an expression ({@code {"@rid": ...}}) is not a literal and is walked like any other node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlAstInspectorRidLiteralTest extends TestHelper {
  private static final int RIDS = 5000;

  private Statement parse(final String sql) {
    return new SQLAntlrParser(database).parseScript(sql).get(0);
  }

  private static String ridList(final int count) {
    final StringBuilder list = new StringBuilder("[");
    for (int i = 0; i < count; i++)
      list.append(i > 0 ? "," : "").append("#1:").append(i);
    return list.append("]").toString();
  }

  @Test
  void aRidListIsOneNodeADeepWalkVisitsOncePerRidAndNotItsTwoNumbers() {
    final Statement statement = parse("SELECT FROM " + ridList(RIDS) + " WHERE a = 1");
    final AtomicInteger rids = new AtomicInteger();
    final AtomicInteger numbers = new AtomicInteger();
    final AtomicInteger all = new AtomicInteger();
    assertThat(SqlAstInspector.allNodesMatch(statement, node -> {
      all.incrementAndGet();
      if (node instanceof Rid)
        rids.incrementAndGet();
      if (node instanceof PInteger)
        numbers.incrementAndGet();
      return true;
    })).isTrue();

    assertThat(rids.get()).isEqualTo(RIDS);
    // The numbers of the statement's own condition, never the ten thousand of the RIDs
    assertThat(numbers.get()).isLessThan(10);
    assertThat(all.get()).isLessThan(RIDS + 100);
  }

  @Test
  void theRestOfTheStatementIsStillWalkedWhenItHasARidList() {
    final Statement statement = parse(
        "SELECT FROM " + ridList(RIDS) + " WHERE a = ? AND b = :named AND out('E').size() > 1 AND c IN (SELECT FROM V WHERE d = ?)");
    final List<Object> found = new ArrayList<>();
    SqlAstInspector.allNodesMatch(statement, node -> {
      if (node instanceof PositionalParameter || node instanceof NamedParameter || node instanceof FunctionCall
          || node instanceof SelectStatement)
        found.add(node);
      return true;
    });
    assertThat(found.stream().filter(PositionalParameter.class::isInstance).count()).isEqualTo(2);
    assertThat(found.stream().filter(NamedParameter.class::isInstance).count()).isEqualTo(1);
    assertThat(found.stream().filter(SelectStatement.class::isInstance).count()).isGreaterThanOrEqualTo(2);

    final List<Statement> nested = new ArrayList<>();
    assertThat(SqlAstInspector.forEachNestedStatement(statement, (inner, enclosing) -> {
      nested.add(inner);
      return true;
    })).isTrue();
    assertThat(nested).hasSize(1);
    assertThat(statement.isIdempotent()).isTrue();
    assertThat(SqlAstInspector.isParallelSafe(statement)).isTrue();
  }

  @Test
  void aRidGivenAsAnExpressionIsNotALeaf() {
    final Statement withParameter = parse("SELECT FROM " + ridList(3) + " WHERE x = 1 AND y IN [{\"@rid\": ?}]");
    final AtomicInteger parameters = new AtomicInteger();
    SqlAstInspector.allNodesMatch(withParameter, node -> {
      if (node instanceof PositionalParameter)
        parameters.incrementAndGet();
      return true;
    });
    assertThat(parameters.get()).isEqualTo(1);

    final Statement withStatement = parse("SELECT FROM [#1:0, {\"@rid\": (SELECT @rid FROM V LIMIT 1)}]");
    final List<Statement> nested = new ArrayList<>();
    SqlAstInspector.forEachNestedStatement(withStatement, (inner, enclosing) -> {
      nested.add(inner);
      return true;
    });
    assertThat(nested).hasSize(1);
  }

  @Test
  void parameterNumbersOfAStatementWithARidListAreStillFound() {
    final Statement statement = parse("SELECT FROM " + ridList(50) + " WHERE a = ? AND b = ?");
    assertThat(SqlAstInspector.parameterNumbersSuffix(statement)).isEmpty();
    statement.checkPositionalParameterCount(2);
  }
}
