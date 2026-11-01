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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11007: the per-hop and per-record work of a fixed-length expand. A type-filtered walk built its bucket mask on every
 * vertex it expanded, every edge-list walk hashed its segment into the transaction's written records even in a transaction
 * that wrote nothing, and every record read asked the transaction's page maps twice. The mask is now kept by the type and the
 * two lookups are skipped when they cannot answer: what the walks return must not change, above all when the type's buckets do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11007EdgeWalkHotPathTest extends TestHelper {

  @Test
  void theBucketMaskOfATypeIsKeptAndRebuiltWhenItsBucketsChange() {
    database.command("sql", "CREATE EDGE TYPE KNOWS").close();
    final String[] knows = { "KNOWS" };

    final EdgeBucketMask first = EdgeBucketMask.of((DatabaseInternal) database, knows);
    assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows)).isSameAs(first);

    // A subtype brings its buckets
    database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS").close();
    final EdgeBucketMask withSubType = EdgeBucketMask.of((DatabaseInternal) database, knows);
    assertThat(withSubType).isNotSameAs(first);
    for (final Bucket bucket : database.getSchema().getType("KNOWS_WELL").getBuckets(false))
      assertThat(withSubType.matches(bucket.getFileId())).isTrue();

    // A bucket added to the type
    database.command("sql", "CREATE BUCKET knows_extra").close();
    database.command("sql", "ALTER TYPE KNOWS BUCKET +knows_extra").close();
    final int extra = database.getSchema().getBucketByName("knows_extra").getFileId();
    assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows).matches(extra)).isTrue();

    // ... and removed from it
    database.command("sql", "ALTER TYPE KNOWS BUCKET -knows_extra").close();
    assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows).matches(extra)).isFalse();

    // Several types, or a name that is not an edge type, are answered as before
    assertThat(EdgeBucketMask.of((DatabaseInternal) database, new String[] { "KNOWS", "KNOWS_WELL" }).matches(extra)).isFalse();
    assertThat(EdgeBucketMask.of((DatabaseInternal) database, new String[] { "NOPE" })).isNull();
  }

  @Test
  void aFilteredHopSeesTheEdgesOfASubTypeCreatedAfterAnEarlierHop() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE KNOWS").close();
    final RID[] people = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < people.length; i++)
        people[i] = database.newVertex("Person").set("id", i).save().getIdentity();
      people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
    });
    assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(1L);

    database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS").close();
    database.transaction(() -> people[0].asVertex().newEdge("KNOWS_WELL", people[2].asVertex()));

    assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(people[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(2L);
    final Iterator<Vertex> neighbours = people[0].asVertex().getVertices(Vertex.DIRECTION.OUT, "KNOWS").iterator();
    int seen = 0;
    while (neighbours.hasNext()) {
      neighbours.next();
      ++seen;
    }
    assertThat(seen).isEqualTo(2);
  }

  @Test
  void theWrittenRecordsAreAskedOnlyOfATransactionThatWroteOne() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE KNOWS").close();
    final RID[] people = new RID[2];
    database.transaction(() -> {
      people[0] = database.newVertex("Person").set("id", 0).save().getIdentity();
      people[1] = database.newVertex("Person").set("id", 1).save().getIdentity();
      people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
    });

    database.transaction(() -> {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      assertThat(tx.hasWrittenRecords()).isFalse();
      assertThat(tx.getWrittenRecord(people[0])).isNull();
      assertThat(people[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(1L);

      // A walk over a list the transaction is writing sees the transaction's own copy
      people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
      assertThat(tx.hasWrittenRecords()).isTrue();
      assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(2L);
    });
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
