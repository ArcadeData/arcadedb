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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #5795.
 * <p>
 * ArcadeDB rejected reads of a node deleted earlier in the same query (property read, SET
 * right-hand-side read) with a {@code CommandExecutionException} and rolled back the whole
 * statement - including the earlier DELETE. However using that same deleted node only as the
 * *target* of a {@code SET}, {@code REMOVE} or label write silently succeeded: the statement
 * reported success, the preceding DELETE was committed, and the write itself had no effect.
 * <p>
 * All access paths that require a deleted node must be validated consistently: using a deleted
 * node as a write target must raise the same {@code DeletedEntityAccess} error as reading from
 * it, and the whole statement (including the DELETE) must roll back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherDeletedNodeWriteTargetIssue5795Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-deleted-node-write-target").create();
    database.getSchema().createVertexType("BugA");
    database.getSchema().createEdgeType("BugRel");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void createNode() {
    database.transaction(() -> database.command("opencypher", "CREATE (t:BugA {_id:'zz', v:0})"));
  }

  private long propertyValue() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (t:BugA {_id:'zz'}) RETURN t.v AS v")) {
      assertThat(rs.hasNext()).as("node must still exist").isTrue();
      return ((Number) rs.next().getProperty("v")).longValue();
    }
  }

  private boolean nodeExists() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (t:BugA {_id:'zz'}) RETURN t")) {
      return rs.hasNext();
    }
  }

  @Test
  void setPropertyOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t SET t.v = 99")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void setPropertyOnDeletedNodeWithoutWithBoundaryAlsoRaisesError() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t SET t.v = 99")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void replaceMapOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t SET t = {v: 99}")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void mergeMapOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t SET t += {v: 99}")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void setLabelOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t SET t:DeletedLabel")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
  }

  @Test
  void removePropertyOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t REMOVE t.v")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void removeLabelOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t REMOVE t:BugA")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
  }

  @Test
  void readingAPropertyFromADeletedNodeStillRaisesErrorAndRollsBack() {
    // Pre-existing behavior (issue's control case): must remain unchanged by the fix.
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t RETURN t.v")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void settingAPropertyOnALiveNodeStillWorks() {
    // Regression guard: the fix must not break ordinary (non-deleted) SET targets.
    createNode();

    database.transaction(() ->
        database.command("opencypher", "MATCH (t:BugA {_id:'zz'}) SET t.v = 99"));

    assertThat(propertyValue()).isEqualTo(99);
  }

  @Test
  void setCaseExpressionTargetOnDeletedNodeRaisesErrorAndRollsBackTheDelete() {
    // The CASE-subclause write target (SET (CASE WHEN ... THEN t END).prop = value, issue #3468)
    // resolves its target via a separate evaluation path (ExpressionEvaluator) that does not go
    // through resolveLatestDoc(), so it needs its own DeletedEntityMarker check.
    createNode();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher",
            "MATCH (t:BugA {_id:'zz'}) DELETE t WITH t SET (CASE WHEN true THEN t END).v = 99")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(nodeExists()).as("DELETE must be rolled back, not silently committed").isTrue();
    assertThat(propertyValue()).isEqualTo(0);
  }

  @Test
  void setPropertyOnDeletedRelationshipRaisesErrorAndRollsBackTheDelete() {
    // The fix is type-generic (DeletedEntityMarker.checkNotDeleted doesn't distinguish vertex vs
    // relationship deletes, and resolveLatestDoc/removeProperty operate on the shared Document
    // supertype), but nothing above exercised a deleted relationship as a SET target.
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:BugA {_id:'a'})-[r:BugRel {v:0}]->(b:BugA {_id:'b'})"));

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher",
            "MATCH (:BugA {_id:'a'})-[r:BugRel]->(:BugA {_id:'b'}) DELETE r WITH r SET r.v = 99")))
        .isInstanceOf(CommandExecutionException.class);

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:BugA {_id:'a'})-[r:BugRel]->(:BugA {_id:'b'}) RETURN r.v AS v")) {
      assertThat(rs.hasNext()).as("relationship DELETE must be rolled back").isTrue();
      assertThat(((Number) rs.next().getProperty("v")).longValue()).isEqualTo(0);
    }
  }
}
