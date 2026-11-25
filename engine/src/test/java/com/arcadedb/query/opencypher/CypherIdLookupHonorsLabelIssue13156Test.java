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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13156: a lookup by record id ({@code WHERE id(n) = x} / {@code elementId(n) = x}) is answered with the record the id
 * names, whatever its type, so {@code MATCH (n:Chunk) WHERE id(n) = rid} bound {@code n} to an Entity when {@code rid} was an
 * Entity's id. A node matches {@code (n:Chunk)} only when it carries the label (Neo4j 5.26 answers no row), a record of a subtype
 * of the label carries it.
 */
class CypherIdLookupHonorsLabelIssue13156Test extends TestHelper {
  private String chunkRid;
  private String subChunkRid;
  private String entityRid;
  private String bothRid;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE VERTEX TYPE SubChunk EXTENDS Chunk");
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE EDGE TYPE LINKS");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Chunk {seq: 1}), (:SubChunk {seq: 2}), (:Entity {seq: 9}), (:Chunk:Entity {seq: 5})"));
    chunkRid = rid("MATCH (n:Chunk) WHERE n.seq = 1 RETURN elementId(n) AS rid");
    subChunkRid = rid("MATCH (n:SubChunk) RETURN elementId(n) AS rid");
    entityRid = rid("MATCH (n:Entity) WHERE n.seq = 9 RETURN elementId(n) AS rid");
    bothRid = rid("MATCH (n:Chunk:Entity) RETURN elementId(n) AS rid");
  }

  @Test
  void aRecordOfAnotherTypeIsNotTheNodeOfTheLabel() {
    for (final String where : List.of("id(n) = rid", "rid = id(n)", "elementId(n) = rid", "rid = elementId(n)")) {
      final Object id = where.contains("elementId") ? entityRid : (Object) numericId(entityRid);
      assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE " + where + " RETURN n.seq AS seq", Map.of("rids", List.of(id))))
          .as(where).isEmpty();
    }
  }

  @Test
  void theOtherShapesOfTheStatementAgree() {
    final Map<String, Object> params = Map.of("rids", List.of(entityRid));
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid AND n.seq > 0 RETURN n.seq AS seq", params)).isEmpty();
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE n.seq > 0 AND elementId(n) = rid RETURN n.seq AS seq", params)).isEmpty();
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk {seq: 9}) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).isEmpty();
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid OPTIONAL MATCH (n)-[:LINKS]->(m) RETURN n.seq AS seq",
        params)).isEmpty();
    assertThat(seqs("MATCH (e:Entity) WHERE e.seq = 9 WITH e MATCH (n:Chunk) WHERE id(n) = id(e) RETURN n.seq AS seq", Map.of())).isEmpty();
    // the same id as a literal, on the clause-by-clause path an OPTIONAL MATCH takes
    assertThat(seqs("MATCH (n:Chunk) WHERE elementId(n) = '" + entityRid + "' OPTIONAL MATCH (n)-[:LINKS]->(m) RETURN n.seq AS seq",
        Map.of())).isEmpty();
    // a label read per row
    assertThat(seqs("UNWIND $rids AS rid UNWIND ['Chunk', 'Entity'] AS label MATCH (n:$(label)) WHERE elementId(n) = rid "
        + "RETURN n.seq AS seq", params)).containsExactly(9);
  }

  @Test
  void anOptionalMatchOfTheLabelBindsNothingForARecordOfAnotherType() {
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND $rids AS rid OPTIONAL MATCH (n:Chunk) WHERE elementId(n) = rid RETURN n IS NULL AS missing", Map.of("rids", List.of(entityRid)))) {
      assertThat(rs.next().<Boolean>getProperty("missing")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aWriteAfterTheMatchDoesNotReachARecordOfAnotherType() {
    final Map<String, Object> params = Map.of("rids", List.of(entityRid));
    database.transaction(() -> {
      database.command("opencypher", "UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid SET n.touched = true", params);
      assertThat(seqs("MATCH (e:Entity) WHERE e.touched IS NOT NULL RETURN e.seq AS seq", Map.of())).isEmpty();

      database.command("opencypher", "UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid DETACH DELETE n", params);
      assertThat(seqs("MATCH (e:Entity) RETURN e.seq AS seq ORDER BY seq", Map.of())).containsExactly(5, 9);
    });
  }

  @Test
  void aRecordThatCarriesTheLabelIsStillTheNode() {
    final Map<String, Object> params = Map.of("rids", List.of(chunkRid, subChunkRid, entityRid, bothRid));

    // a record of a subtype carries the label of its supertype, as in a scan of the type
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).containsExactly(1, 2, 5);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:SubChunk) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).containsExactly(2);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Entity) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).containsExactly(9, 5);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).containsExactly(1, 2, 9, 5);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid AND n.seq > 1 RETURN n.seq AS seq", params))
        .containsExactly(2, 5);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid OPTIONAL MATCH (n)-[:LINKS]->(m) RETURN n.seq AS seq",
        params)).containsExactly(1, 2, 5);
  }

  @Test
  void severalLabelsAreStillEachRequiredOrAlternatives() {
    final Map<String, Object> params = Map.of("rids", List.of(chunkRid, entityRid, bothRid));
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk:Entity) WHERE elementId(n) = rid RETURN n.seq AS seq", params)).containsExactly(5);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk|Entity) WHERE elementId(n) = rid RETURN n.seq AS seq", params))
        .containsExactly(1, 9, 5);
  }

  @Test
  void aNullOrInvalidIdAnswersNoRow() {
    final List<Object> rids = new ArrayList<>();
    rids.add(null);
    rids.add("not-an-id");
    rids.add("#99:99");
    rids.add(chunkRid);
    assertThat(seqs("UNWIND $rids AS rid MATCH (n:Chunk) WHERE elementId(n) = rid RETURN n.seq AS seq", Map.of("rids", rids)))
        .containsExactly(1);
  }

  private String rid(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return rs.next().getProperty("rid");
    }
  }

  private long numericId(final String rid) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) WHERE elementId(n) = $rid RETURN id(n) AS id", Map.of("rid", rid))) {
      return ((Number) rs.next().getProperty("id")).longValue();
    }
  }

  private List<Integer> seqs(final String cypher, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(((Number) r.getProperty("seq")).intValue());
      }
    }
    return out;
  }
}
