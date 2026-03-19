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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.ast.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for StatementBuilder.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementBuilderTest {

  @Test
  void emptyStatement() {
    final StatementBuilder builder = new StatementBuilder();
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt).isNotNull();
    assertThat(stmt.hasCreate()).isFalse();
    assertThat(stmt.hasMerge()).isFalse();
    assertThat(stmt.hasDelete()).isFalse();
    assertThat(stmt.hasRemove()).isFalse();
  }

  @Test
  void addMatch() {
    final StatementBuilder builder = new StatementBuilder();
    final MatchClause match = new MatchClause(Collections.emptyList(), false, null);

    builder.addMatch(match);
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.getMatchClauses()).hasSize(1);
    assertThat(stmt.getMatchClauses().get(0)).isEqualTo(match);
  }

  @Test
  void setCreate() {
    final StatementBuilder builder = new StatementBuilder();
    final CreateClause create = new CreateClause(Collections.emptyList());

    builder.setCreate(create);
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.hasCreate()).isTrue();
    assertThat(stmt.getCreateClause()).isEqualTo(create);
  }

  @Test
  void setDelete() {
    final StatementBuilder builder = new StatementBuilder();
    final DeleteClause delete = new DeleteClause(Collections.singletonList("n"), false);

    builder.setDelete(delete);
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.hasDelete()).isTrue();
    assertThat(stmt.getDeleteClause()).isEqualTo(delete);
  }

  @Test
  void setMerge() {
    final StatementBuilder builder = new StatementBuilder();
    final NodePattern node = new NodePattern("n", null, null);
    final PathPattern path = new PathPattern(node);
    final MergeClause merge = new MergeClause(path, null, null);

    builder.setMerge(merge);
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.hasMerge()).isTrue();
    assertThat(stmt.getMergeClause()).isEqualTo(merge);
  }

  @Test
  void addRemove() {
    final StatementBuilder builder = new StatementBuilder();
    final List<RemoveClause.RemoveItem> items = new ArrayList<>();
    items.add(new RemoveClause.RemoveItem("n", "prop"));
    final RemoveClause remove = new RemoveClause(items);

    builder.addRemove(remove);
    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.hasRemove()).isTrue();
    assertThat(stmt.getRemoveClauses()).hasSize(1);
  }

  @Test
  void setOrderBySkipLimit() {
    final StatementBuilder builder = new StatementBuilder();
    final OrderByClause orderBy = new OrderByClause(Collections.emptyList());

    builder.setOrderBy(orderBy);
    builder.setSkip(new LiteralExpression(10, "10"));
    builder.setLimit(new LiteralExpression(100, "100"));

    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.getOrderByClause()).isEqualTo(orderBy);
    assertThat(stmt.getSkip().getText()).isEqualTo("10");
    assertThat(stmt.getLimit().getText()).isEqualTo("100");
  }

  @Test
  void clauseOrdering() {
    final StatementBuilder builder = new StatementBuilder();

    final MatchClause match = new MatchClause(Collections.emptyList(), false, null);
    final CreateClause create = new CreateClause(Collections.emptyList());
    final List<ReturnClause.ReturnItem> items = new ArrayList<>();
    items.add(new ReturnClause.ReturnItem(new VariableExpression("n"), "n"));
    final ReturnClause returnClause = new ReturnClause(items, false);

    // Add in specific order
    builder.addMatch(match);
    builder.setCreate(create);
    builder.setReturn(returnClause);

    final SimpleCypherStatement stmt = builder.build();

    // Verify clauses are tracked in order
    final List<ClauseEntry> clausesInOrder = stmt.getClausesInOrder();
    assertThat(clausesInOrder).hasSize(3);
    assertThat(clausesInOrder.get(0).getType()).isEqualTo(ClauseEntry.ClauseType.MATCH);
    assertThat(clausesInOrder.get(1).getType()).isEqualTo(ClauseEntry.ClauseType.CREATE);
    assertThat(clausesInOrder.get(2).getType()).isEqualTo(ClauseEntry.ClauseType.RETURN);
  }

  @Test
  void multipleMatchClauses() {
    final StatementBuilder builder = new StatementBuilder();

    final MatchClause match1 = new MatchClause(Collections.emptyList(), false, null);
    final MatchClause match2 = new MatchClause(Collections.emptyList(), true, null);

    builder.addMatch(match1);
    builder.addMatch(match2);

    final SimpleCypherStatement stmt = builder.build();

    assertThat(stmt.getMatchClauses()).hasSize(2);
    assertThat(stmt.getMatchClauses().get(0)).isEqualTo(match1);
    assertThat(stmt.getMatchClauses().get(1)).isEqualTo(match2);
  }
}
