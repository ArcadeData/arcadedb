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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #11693: a map projection over an unbound variable evaluated to null instead of being
 * refused, and a map projection was accepted as the receiver of a SET property assignment, which then did nothing.
 */
class CypherMapProjectionReceiverIssue11693Test extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Node {id:1,v:1,s:'a',b:true,l:[1,2]})
        CREATE (b:Node {id:2,v:2,s:'b',b:false,l:[]})
        CREATE (c:Node {id:3,v:3,s:'c',b:true,l:[3]})
        CREATE (a)-[:R {k:1}]->(b)
        CREATE (b)-[:R {k:2}]->(c)""").close());
  }

  @Test
  void propertyAccessOnAnUnboundMapProjectionIsRefused() {
    assertThatThrownBy(() -> query("RETURN (n {id:1}).id AS id"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Variable 'n' not defined");
  }

  @Test
  void anUnboundMapProjectionIsRefused() {
    assertThatThrownBy(() -> query("RETURN n {.id} AS m"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Variable 'n' not defined");
  }

  @Test
  void theIssueQueryWithAnUnboundMapProjectionIsRefused() {
    assertThatThrownBy(() -> command("""
        FOREACH (x IN [1,2] | SET (n {id:1}).s = 'upd')
        RETURN (n {id:1}).id AS id, (n {id:1}).s AS s"""))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void aMapProjectionIsNotAnAssignableSetReceiver() {
    assertThatThrownBy(() -> command("""
        MATCH (n:Node {id:1})
        FOREACH (x IN [1,2] | SET (n {id:1}).s = 'upd')
        RETURN n.id AS id, n.s AS s"""))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expected Node or Relationship but was Map");
    assertThat(query("MATCH (n:Node {id:1}) RETURN n.s AS s").getFirst().<String>getProperty("s")).isEqualTo("a");
  }

  @Test
  void aMapProjectionIsNotAnAssignableSetReceiverOutsideForeach() {
    assertThatThrownBy(() -> command("MATCH (n:Node {id:1}) SET (n {id:1}).s = 'upd' RETURN n.s AS s"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expected Node or Relationship but was Map");
  }

  @Test
  void aReceiverThatEvaluatesToAMapAtRunTimeIsATypeError() {
    // The CASE hides the kind of the receiver from the validator: the SET must still refuse the map once it is known,
    // rather than skip the write
    assertThatThrownBy(() -> command("MATCH (n:Node {id:1}) SET (CASE WHEN true THEN n {.id} END).s = 'upd'"))
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("expected Node or Relationship but was MAP");
    assertThat(query("MATCH (n:Node {id:1}) RETURN n.s AS s").getFirst().<String>getProperty("s")).isEqualTo("a");
  }

  @Test
  void aReceiverThatEvaluatesToANodeAtRunTimeStillWrites() {
    command("MATCH (n:Node {id:1}) SET (CASE WHEN true THEN n END).s = 'upd'");
    assertThat(query("MATCH (n:Node {id:1}) RETURN n.s AS s").getFirst().<String>getProperty("s")).isEqualTo("upd");
  }

  @Test
  void aReceiverThatEvaluatesToNullAtRunTimeIsStillANoOp() {
    command("MATCH (n:Node {id:1}) SET (CASE WHEN false THEN n END).s = 'upd'");
    assertThat(query("MATCH (n:Node {id:1}) RETURN n.s AS s").getFirst().<String>getProperty("s")).isEqualTo("a");
  }

  @Test
  void aBoundMapProjectionStillResolves() {
    final List<Result> rows = query("MATCH (n:Node {id:1}) RETURN (n {id:1}).id AS id");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("id")).isEqualTo(1L);
  }

  @Test
  void aBoundMapProjectionWithAPropertySelectorStillResolves() {
    final List<Result> rows = query("MATCH (n:Node {id:1}) RETURN n {.id, .s} AS m");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("m")).isEqualTo(java.util.Map.of("id", 1, "s", "a"));
  }

  @Test
  void aSetOnTheBoundVariableItselfStillWrites() {
    command("MATCH (n:Node {id:1}) FOREACH (x IN [1,2] | SET n.s = 'upd')");
    assertThat(query("MATCH (n:Node {id:1}) RETURN n.s AS s").getFirst().<String>getProperty("s")).isEqualTo("upd");
  }

  @Test
  void theColonLabelFormIsASyntaxError() {
    assertThatThrownBy(() -> query("RETURN (n:Node {id:1}).id AS id"))
        .isInstanceOf(CommandParsingException.class);
  }

  private List<Result> query(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return rs.stream().toList();
    }
  }

  private void command(final String cypher) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        rs.stream().toList();
      }
    });
  }
}
