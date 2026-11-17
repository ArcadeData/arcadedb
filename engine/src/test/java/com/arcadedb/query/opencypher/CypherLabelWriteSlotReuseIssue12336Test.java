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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12336: a Cypher label write rewrites the vertex under a new RID and keeps a statement-wide map from the original RID
 * to the replacement. Under {@code CALL { ... } IN TRANSACTIONS} each batch commits, and the engine handed the slot a
 * committed delete freed to a later insert: a plain {@code CREATE} of a later batch took the replaced vertex's RID, and a
 * later MATCH of the same statement that read the new vertex back was redirected to the old vertex's replacement, so the SET
 * landed on the wrong record. The slots of the replaced records are now held until the statement ends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteSlotReuseIssue12336Test extends TestHelper {

  private static final String STATEMENT = """
      MATCH (n:A) CALL (n) { SET n:B REMOVE n:A } IN TRANSACTIONS OF 1 ROW
      WITH count(*) AS c
      CALL () { CREATE (:A {fresh: true}) } IN TRANSACTIONS OF 1 ROW
      WITH c
      MATCH (x:A {fresh: true}) SET x.touched = true
      RETURN x""";

  @Override
  protected void beginTest() {
    // One bucket, so the new A vertex is offered the slot the relabelled one freed
    database.command("sql", "CREATE VERTEX TYPE A BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE B BUCKETS 1");
  }

  @Test
  void aCreateInALaterBatchIsNotRedirectedToTheReplacementOfTheRecordWhoseSlotItWasOffered() {
    final RID freed = createOriginal();

    final List<Result> rows = run(STATEMENT);

    assertThat(rows).hasSize(1);
    // The row answers the new A vertex, not the relabelled vertex its binding was redirected to
    final Vertex returned = rows.getFirst().<Identifiable>getProperty("x").asVertex();
    assertThat(returned.getTypeName()).isEqualTo("A");
    assertThat(returned.getBoolean("fresh")).isTrue();
    assertThat(returned.getIdentity()).isNotEqualTo(freed);

    // The SET reached the new A vertex, and the relabelled vertex was left alone
    assertThat(run("MATCH (x:A) RETURN x.fresh AS fresh, x.touched AS touched")).singleElement().satisfies(row -> {
      assertThat(row.<Boolean>getProperty("fresh")).isTrue();
      assertThat(row.<Boolean>getProperty("touched")).isTrue();
    });
    assertThat(run("MATCH (y:B) RETURN y.name AS name, y.touched AS touched")).singleElement().satisfies(row -> {
      assertThat(row.<String>getProperty("name")).isEqualTo("n1");
      assertThat(row.<Object>getProperty("touched")).isNull();
    });
  }

  /**
   * The hold lasts as long as the statement, not longer: once it ended the slot is the engine's again, which also shows the
   * test above is not vacuous - the engine does offer that slot to the next A vertex.
   */
  @Test
  void theSlotIsHeldOnlyWhileTheStatementRuns() {
    final RID freed = createOriginal();

    run(STATEMENT);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(freed.getBucketId());
    assertThat(bucket.isPositionHeld(freed.getPosition())).isFalse();

    final RID[] next = new RID[1];
    database.begin();
    next[0] = database.newVertex("A").save().getIdentity();
    database.rollback();
    assertThat(next[0]).as("the next A vertex takes the slot the statement released").isEqualTo(freed);
  }

  /** A statement that fails still gives the slots back. */
  @Test
  void aFailedStatementReleasesTheSlotsItHeld() {
    final RID freed = createOriginal();

    try {
      run("""
          MATCH (n:A) CALL (n) { SET n:B REMOVE n:A } IN TRANSACTIONS OF 1 ROW
          WITH count(*) AS c
          RETURN 1 / (c - c) AS boom""");
    } catch (final RuntimeException expected) {
      // division by zero after the batches committed
    }

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(freed.getBucketId());
    assertThat(bucket.isPositionHeld(freed.getPosition())).isFalse();
  }

  private RID createOriginal() {
    final Vertex[] original = new Vertex[1];
    database.transaction(() -> original[0] = database.newVertex("A").set("name", "n1").save());
    return original[0].getIdentity();
  }

  private List<Result> run(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
