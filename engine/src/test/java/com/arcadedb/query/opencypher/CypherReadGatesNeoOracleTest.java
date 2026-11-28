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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The read statements of the modelhub / Graphiti ingest and search lanes that decide a row by a 3-valued predicate: the vector-staleness gate
 * ({@code n.embedding IS NULL OR n.embedding_model <> row.model OR row.force = true}), the "needs a write" test over {@code keys(row.props)}, the
 * chunk and question gates behind an {@code UNWIND ... MATCH ({external_id: row.eid})} seek, the anchor-record flow ending in {@code LIMIT}, {@code x IN $list}
 * with nulls and mixed numeric kinds, string predicates over non-strings, list predicates over a missing list, aggregation over OPTIONAL MATCH rows, and
 * an OPTIONAL MATCH read through a variable an earlier one left null.
 * <p>
 * Every expected row is the answer of Neo4j 5.26 to the same data script and statement. Each statement runs on a database without an index and on one with
 * unique indexes on the seek keys, so the index seek and the scan must agree with it and with each other.
 */
class CypherReadGatesNeoOracleTest {
  private static final String[] DATA = {
      "CREATE (:Entity {normalized_name:'e1', external_id:'E1', embedding:[0.5,0.25], embedding_model:'m1', created_at:100, deleted:false, k1:1, k2:'a', k3:[1,2]})",
      "CREATE (:Entity {normalized_name:'e2', external_id:'E2', embedding_model:'m1', created_at:100, deleted:false})",
      "CREATE (:Entity {normalized_name:'e3', external_id:'E3', embedding:[0.5], created_at:100, deleted:false})",
      "CREATE (:Entity {normalized_name:'e4', external_id:'E4', embedding:[0.5], embedding_model:'m2', created_at:100, deleted:false})",
      "CREATE (:Entity {normalized_name:'e5', external_id:'E5', embedding:[0.5], embedding_model:'m1', created_at:100, deleted:true})",
      "CREATE (:Entity {normalized_name:'e6', external_id:'E6', embedding:[0.5], embedding_model:'m1', created_at:100, deleted:false, deleted_at:5})",
      "CREATE (:Entity {normalized_name:'e7', embedding:[0.5], embedding_model:'m1', created_at:100, deleted:false})",
      "CREATE (:Entity {normalized_name:'e8', external_id:'E8', embedding:[0.5], embedding_model:'m1', deleted:false})",
      "CREATE (:Entity {normalized_name:'e9', external_id:'E9', embedding:[0.5], embedding_model:'m1', created_at:100, k1:1.0, k2:'a'})",
      "CREATE (:Record {external_id:'r1', graph_name:'g1'}), (:Record {external_id:'r2', graph_name:'g2', deleted_at:7}), (:Record {external_id:'r3'}), (:Record {external_id:'r4', graph_name:'g1'}), (:Record {external_id:'r5', graph_name:'g1'})",
      "CREATE (:Chunk {external_id:'c1', graph_name:'g1', embedding:[0.5], embedding_model:'m1', page_number:1, section_heading:'Intro text', is_table:true, score:1, access_principals:['p1','p2']}), (:Chunk {external_id:'c2', embedding_model:'m1', page_number:2.0, section_heading:'Résumé', score:2.5, access_principals:[]}), (:Chunk {external_id:'c3', graph_name:'g2', embedding:[0.5], page_number:'a', is_table:false, access_principals:['p1','p1','p3'], deleted_at:9}), (:Chunk {external_id:'c4', embedding:[0.5], embedding_model:'m2', question_embedding:[0.5], question_embedding_model:'m1', question_embedding_stale:false, page_number:1.0, score:2}), (:Chunk {external_id:'c5', embedding:[0.5], embedding_model:'m1', question_embedding:[0.5], question_embedding_stale:true, section_heading:'', score:0}), (:Chunk {external_id:'c6', embedding:[0.5], embedding_model:'m1', question_embedding:[0.5], question_embedding_model:'m3', question_embedding_stale:false, page_number:3}), (:Chunk {external_id:'c7', embedding:[0.5], embedding_model:'m1', question_embedding:[0.5], question_embedding_model:'m1'})",
      "MATCH (c:Chunk), (r:Record) WHERE (c.external_id IN ['c1','c2'] AND r.external_id = 'r1') OR (c.external_id IN ['c3','c4','c7'] AND r.external_id = 'r2') OR (c.external_id = 'c1' AND r.external_id = 'r3') OR (c.external_id = 'c5' AND r.external_id = 'r4') CREATE (c)-[:OF_RECORD]->(r)"
  };

  private Database plain;
  private Database indexed;

  @BeforeEach
  void setup() {
    plain = new DatabaseFactory("./target/databases/cypherReadGatesPlain").create();
    indexed = new DatabaseFactory("./target/databases/cypherReadGatesIndexed").create();
    indexed.getSchema().createVertexType("Chunk");
    indexed.getSchema().createVertexType("Record");
    indexed.getSchema().createVertexType("Entity");
    indexed.command("sql", "CREATE PROPERTY Chunk.external_id STRING").close();
    indexed.command("sql", "CREATE INDEX ON Chunk (external_id) UNIQUE").close();
    indexed.command("sql", "CREATE PROPERTY Record.external_id STRING").close();
    indexed.command("sql", "CREATE INDEX ON Record (external_id) UNIQUE").close();
    indexed.command("sql", "CREATE PROPERTY Entity.normalized_name STRING").close();
    indexed.command("sql", "CREATE INDEX ON Entity (normalized_name) UNIQUE").close();
    for (final Database db : new Database[] { plain, indexed })
      for (final String statement : DATA)
        db.transaction(() -> db.command("opencypher", statement).close());
  }

  @AfterEach
  void teardown() {
    for (final Database db : new Database[] { plain, indexed })
      if (db != null)
        db.drop();
  }

  private void check(final String query, final Map<String, Object> params, final String[] columns, final String... expected) {
    for (final Database db : new Database[] { plain, indexed })
      assertThat(rows(db, query, params, columns)).as(db == plain ? "without index" : "with unique indexes").containsExactly(expected);
  }

  private static List<String> rows(final Database db, final String query, final Map<String, Object> params, final String[] columns) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = db.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final List<String> values = new ArrayList<>(columns.length);
        for (final String column : columns)
          values.add(String.valueOf(row.<Object>getProperty(column)));
        rows.add(String.join(",", values));
      }
    }
    return rows;
  }

  private static Map<String, Object> map(final Object... keysAndValues) {
    final Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return map;
  }

  private static List<Object> list(final Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }

  @Test
  void entityGateMatch() {
    check("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.norm}) WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale WITH n, row, vector_stale, (vector_stale OR (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)))) AS wrote RETURN row.i AS i, row.norm AS norm, vector_stale, wrote ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i", "norm", "vector_stale", "wrote" },
        "1,e1,false,false",
        "2,e1,null,null",
        "3,e1,true,true",
        "4,e1,true,true",
        "5,e1,null,null",
        "6,e1,false,true",
        "7,e1,false,true",
        "8,e1,false,false",
        "9,e1,false,true",
        "10,e1,false,true",
        "11,e1,false,null",
        "12,e1,false,null",
        "13,e2,true,true",
        "14,e3,true,true",
        "15,e4,true,true",
        "16,e5,false,true",
        "17,e6,false,true",
        "18,e7,false,true",
        "19,e8,false,true",
        "20,e9,false,true",
        "21,e9,null,true");
  }

  @Test
  void entityGateOptionalMatch() {
    check("UNWIND $rows AS row OPTIONAL MATCH (n:Entity {normalized_name: row.norm}) WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale WITH n, row, vector_stale, (vector_stale OR (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)))) AS wrote RETURN row.i AS i, row.norm AS norm, vector_stale, wrote ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i", "norm", "vector_stale", "wrote" },
        "1,e1,false,false",
        "2,e1,null,null",
        "3,e1,true,true",
        "4,e1,true,true",
        "5,e1,null,null",
        "6,e1,false,true",
        "7,e1,false,true",
        "8,e1,false,false",
        "9,e1,false,true",
        "10,e1,false,true",
        "11,e1,false,null",
        "12,e1,false,null",
        "13,e2,true,true",
        "14,e3,true,true",
        "15,e4,true,true",
        "16,e5,false,true",
        "17,e6,false,true",
        "18,e7,false,true",
        "19,e8,false,true",
        "20,e9,false,true",
        "21,e9,null,true",
        "22,e0,true,true",
        "23,e0,true,true");
  }

  @Test
  void entityGateAsWhere() {
    check("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.norm}) WHERE n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true RETURN row.i AS i ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i" },
        "3",
        "4",
        "13",
        "14",
        "15");
  }

  @Test
  void entityGateNegated() {
    check("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.norm}) WHERE NOT (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) RETURN row.i AS i ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i" },
        "1",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "16",
        "17",
        "18",
        "19",
        "20");
  }

  @Test
  void entityNeedsWriteAsWhere() {
    check("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.norm}) WHERE n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)) RETURN row.i AS i ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i" },
        "6",
        "7",
        "9",
        "10",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21");
  }

  @Test
  void entityNeedsWriteNegated() {
    check("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.norm}) WHERE NOT (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false))) RETURN row.i AS i ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i" },
        "1",
        "2",
        "3",
        "4",
        "5",
        "8",
        "13",
        "14",
        "15");
  }

  @Test
  void entityNeedsWriteOptional() {
    check("UNWIND $rows AS row OPTIONAL MATCH (n:Entity {normalized_name: row.norm}) WHERE n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)) RETURN row.i AS i, n.normalized_name AS norm ORDER BY i",
        map("rows", list(map("i", 1L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 2L, "norm", "e1", "model", "m1", "force", null, "props", map("k1", 1L)), map("i", 3L, "norm", "e1", "model", "m1", "force", true, "props", map()), map("i", 4L, "norm", "e1", "model", "m2", "force", false, "props", map()), map("i", 5L, "norm", "e1", "model", null, "force", false, "props", map("k1", 1.0)), map("i", 6L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", null)), map("i", 7L, "norm", "e1", "model", "m1", "force", false, "props", map("k1", 2L)), map("i", 8L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(1L, 2L))), map("i", 9L, "norm", "e1", "model", "m1", "force", false, "props", map("k3", list(2L, 1L))), map("i", 10L, "norm", "e1", "model", "m1", "force", false, "props", map("zz", "q")), map("i", 11L, "norm", "e1", "model", "m1", "force", false), map("i", 12L, "norm", "e1", "model", "m1", "force", false, "props", null), map("i", 13L, "norm", "e2", "model", "m1", "force", false, "props", map()), map("i", 14L, "norm", "e3", "model", "m1", "force", false, "props", map()), map("i", 15L, "norm", "e4", "model", "m1", "force", false, "props", map()), map("i", 16L, "norm", "e5", "model", "m1", "force", false, "props", map()), map("i", 17L, "norm", "e6", "model", "m1", "force", false, "props", map()), map("i", 18L, "norm", "e7", "model", "m1", "force", false, "props", map()), map("i", 19L, "norm", "e8", "model", "m1", "force", false, "props", map()), map("i", 20L, "norm", "e9", "model", "m1", "force", false, "props", map("k1", 1L, "k2", "a")), map("i", 21L, "norm", "e9", "model", "m1", "force", null, "props", map("k1", 1.0)), map("i", 22L, "norm", "e0", "model", "m1", "force", false, "props", map()), map("i", 23L, "norm", "e0", "model", null, "force", null, "props", null))),
        new String[] { "i", "norm" },
        "1,null",
        "2,null",
        "3,null",
        "4,null",
        "5,null",
        "6,e1",
        "7,e1",
        "8,null",
        "9,e1",
        "10,e1",
        "11,null",
        "12,null",
        "13,null",
        "14,null",
        "15,null",
        "16,e5",
        "17,e6",
        "18,e7",
        "19,e8",
        "20,e9",
        "21,e9",
        "22,null",
        "23,null");
  }

  @Test
  void chunkEmbeddingGate() {
    check("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) WHERE c.embedding IS NULL OR c.embedding_model IS NULL OR c.embedding_model <> row.model RETURN row.i AS i, c.external_id AS id ORDER BY i",
        map("rows", list(map("i", 1L, "eid", "c1", "model", "m1"), map("i", 2L, "eid", "c2", "model", "m1"), map("i", 3L, "eid", "c3", "model", "m1"), map("i", 4L, "eid", "c4", "model", "m1"), map("i", 5L, "eid", "c5", "model", null), map("i", 6L, "eid", "c6", "model", "m3"), map("i", 7L, "eid", "c7", "model", "m1"), map("i", 8L, "eid", null, "model", "m1"), map("i", 9L, "model", "m1"), map("i", 10L, "eid", "nope", "model", "m1"), map("i", 11L, "eid", "c1", "model", "m2"), map("i", 12L, "eid", "c1", "model", null))),
        new String[] { "i", "id" },
        "2,c2",
        "3,c3",
        "4,c4",
        "6,c6",
        "11,c1");
  }

  @Test
  void chunkEmbeddingGateOptional() {
    check("UNWIND $rows AS row OPTIONAL MATCH (c:Chunk {external_id: row.eid}) WHERE c.embedding IS NULL OR c.embedding_model IS NULL OR c.embedding_model <> row.model RETURN row.i AS i, c.external_id AS id ORDER BY i",
        map("rows", list(map("i", 1L, "eid", "c1", "model", "m1"), map("i", 2L, "eid", "c2", "model", "m1"), map("i", 3L, "eid", "c3", "model", "m1"), map("i", 4L, "eid", "c4", "model", "m1"), map("i", 5L, "eid", "c5", "model", null), map("i", 6L, "eid", "c6", "model", "m3"), map("i", 7L, "eid", "c7", "model", "m1"), map("i", 8L, "eid", null, "model", "m1"), map("i", 9L, "model", "m1"), map("i", 10L, "eid", "nope", "model", "m1"), map("i", 11L, "eid", "c1", "model", "m2"), map("i", 12L, "eid", "c1", "model", null))),
        new String[] { "i", "id" },
        "1,null",
        "2,c2",
        "3,c3",
        "4,c4",
        "5,null",
        "6,c6",
        "7,null",
        "8,null",
        "9,null",
        "10,null",
        "11,c1",
        "12,null");
  }

  @Test
  void chunkQuestionGate() {
    check("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) WHERE c.question_embedding IS NULL OR coalesce(c.question_embedding_model, '') <> row.model OR c.question_embedding_stale = true RETURN row.i AS i, c.external_id AS id ORDER BY i",
        map("rows", list(map("i", 1L, "eid", "c1", "model", "m1"), map("i", 2L, "eid", "c2", "model", "m1"), map("i", 3L, "eid", "c3", "model", "m1"), map("i", 4L, "eid", "c4", "model", "m1"), map("i", 5L, "eid", "c5", "model", null), map("i", 6L, "eid", "c6", "model", "m3"), map("i", 7L, "eid", "c7", "model", "m1"), map("i", 8L, "eid", null, "model", "m1"), map("i", 9L, "model", "m1"), map("i", 10L, "eid", "nope", "model", "m1"), map("i", 11L, "eid", "c1", "model", "m2"), map("i", 12L, "eid", "c1", "model", null))),
        new String[] { "i", "id" },
        "1,c1",
        "2,c2",
        "3,c3",
        "5,c5",
        "11,c1",
        "12,c1");
  }

  @Test
  void chunkQuestionGateNegated() {
    check("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) WHERE NOT (c.question_embedding IS NULL OR coalesce(c.question_embedding_model, '') <> row.model OR c.question_embedding_stale = true) RETURN row.i AS i, c.external_id AS id ORDER BY i",
        map("rows", list(map("i", 1L, "eid", "c1", "model", "m1"), map("i", 2L, "eid", "c2", "model", "m1"), map("i", 3L, "eid", "c3", "model", "m1"), map("i", 4L, "eid", "c4", "model", "m1"), map("i", 5L, "eid", "c5", "model", null), map("i", 6L, "eid", "c6", "model", "m3"), map("i", 7L, "eid", "c7", "model", "m1"), map("i", 8L, "eid", null, "model", "m1"), map("i", 9L, "model", "m1"), map("i", 10L, "eid", "nope", "model", "m1"), map("i", 11L, "eid", "c1", "model", "m2"), map("i", 12L, "eid", "c1", "model", null))),
        new String[] { "i", "id" },
        "4,c4",
        "6,c6");
  }

  @Test
  void anchorFlow_graphs() {
    check("UNWIND $anchor_ids AS a MATCH (r:Record {external_id: a}) WHERE r.deleted_at IS NULL AND coalesce(r.graph_name, '') IN $graphs WITH r MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.deleted_at IS NULL RETURN r.external_id AS rid, c.external_id AS cid ORDER BY rid, cid LIMIT $n",
        map("anchor_ids", list("r1", "r2", null, "r3", "r4", "nope", "r1"), "graphs", list("g1", null), "n", 50L),
        new String[] { "rid", "cid" },
        "r1,c1",
        "r1,c1",
        "r1,c2",
        "r1,c2",
        "r4,c5");
  }

  @Test
  void anchorFlow_graphsAndRecords() {
    check("UNWIND $anchor_ids AS a MATCH (r:Record {external_id: a}) WHERE r.deleted_at IS NULL AND r.external_id IN $record_ids AND coalesce(r.graph_name, '') IN $graphs WITH r MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.deleted_at IS NULL RETURN r.external_id AS rid, c.external_id AS cid ORDER BY rid, cid LIMIT $n",
        map("anchor_ids", list("r1", "r2", null, "r3", "r4", "nope", "r1"), "graphs", list("g1", null), "n", 50L, "record_ids", list("r1", "r3", null)),
        new String[] { "rid", "cid" },
        "r1,c1",
        "r1,c1",
        "r1,c2",
        "r1,c2");
  }

  @Test
  void anchorFlowLimit() {
    check("UNWIND $anchor_ids AS a MATCH (r:Record {external_id: a}) WHERE r.deleted_at IS NULL WITH r MATCH (r)<-[:OF_RECORD]-(c:Chunk) RETURN DISTINCT r.external_id AS rid LIMIT $n",
        map("anchor_ids", list("r1", "r2", null, "r3", "r4", "nope", "r1"), "n", 2L),
        new String[] { "rid" },
        "r1",
        "r3");
  }

  @Test
  void anchorFlowGraphIn() {
    check("UNWIND $anchor_ids AS a MATCH (r:Record {external_id: a}) WHERE r.graph_name IN $graphs RETURN a, r.external_id AS rid ORDER BY a, rid",
        map("anchor_ids", list("r1", "r2", null, "r3", "r4", "nope", "r1"), "graphs", list("g1", "g2")),
        new String[] { "a", "rid" },
        "r1,r1",
        "r1,r1",
        "r2,r2",
        "r4,r4");
  }

  @Test
  void anchorFlowNotIn() {
    check("UNWIND $anchor_ids AS a MATCH (r:Record {external_id: a}) WHERE NOT r.graph_name IN $graphs RETURN a, r.external_id AS rid ORDER BY a, rid",
        map("anchor_ids", list("r1", "r2", null, "r3", "r4", "nope", "r1"), "graphs", list("g1", null)),
        new String[] { "a", "rid" });
  }

  @Test
  void filter_inNullList() {
    check("MATCH (c:Chunk) WHERE c.external_id IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list(new Object[] { null })),
        new String[] { "id" });
  }

  @Test
  void filter_inMixedList() {
    check("MATCH (c:Chunk) WHERE c.external_id IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list("c1", null, 5L, 2.0)),
        new String[] { "id" },
        "c1");
  }

  @Test
  void filter_inEmptyParam() {
    check("MATCH (c:Chunk) WHERE c.external_id IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list()),
        new String[] { "id" });
  }

  @Test
  void filter_notInEmptyParam() {
    check("MATCH (c:Chunk) WHERE NOT c.external_id IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list()),
        new String[] { "id" },
        "c1",
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_notInEmptyLiteralMissingProperty() {
    check("MATCH (c:Chunk) WHERE NOT (c.page_number IN []) RETURN c.external_id AS id ORDER BY id",
        map(),
        new String[] { "id" },
        "c1",
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_notInEmptyParamMissingProperty() {
    check("MATCH (c:Chunk) WHERE NOT c.page_number IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list()),
        new String[] { "id" },
        "c1",
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_pagesMixedKinds() {
    check("MATCH (c:Chunk) WHERE c.page_number IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list(1L, 2.0, null)),
        new String[] { "id" },
        "c1",
        "c2",
        "c4");
  }

  @Test
  void filter_pagesMixedKindsNegated() {
    check("MATCH (c:Chunk) WHERE NOT c.page_number IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list(1L, 2.0, null)),
        new String[] { "id" });
  }

  @Test
  void filter_pagesStringAndInt() {
    check("MATCH (c:Chunk) WHERE c.page_number IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list("a", 3L)),
        new String[] { "id" },
        "c3",
        "c6");
  }

  @Test
  void filter_scoreIntegralDouble() {
    check("MATCH (c:Chunk) WHERE c.score IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list(1.0, 2L)),
        new String[] { "id" },
        "c1",
        "c4");
  }

  @Test
  void filter_scoreNotInWithNull() {
    check("MATCH (c:Chunk) WHERE NOT c.score IN $l RETURN c.external_id AS id ORDER BY id",
        map("l", list(1L, null)),
        new String[] { "id" });
  }

  @Test
  void filter_startsWithOnNonString() {
    check("MATCH (c:Chunk) WHERE c.page_number STARTS WITH '1' RETURN c.external_id AS id ORDER BY id",
        map(),
        new String[] { "id" });
  }

  @Test
  void filter_startsWithNullParam() {
    check("MATCH (c:Chunk) WHERE c.section_heading STARTS WITH $s RETURN c.external_id AS id ORDER BY id",
        map("s", null),
        new String[] { "id" });
  }

  @Test
  void filter_startsWithEmpty() {
    check("MATCH (c:Chunk) WHERE c.section_heading STARTS WITH $s RETURN c.external_id AS id ORDER BY id",
        map("s", ""),
        new String[] { "id" },
        "c1",
        "c2",
        "c5");
  }

  @Test
  void filter_containsLowered() {
    check("MATCH (c:Chunk) WHERE toLower(coalesce(c.section_heading, '')) CONTAINS $s RETURN c.external_id AS id ORDER BY id",
        map("s", "ntro"),
        new String[] { "id" },
        "c1");
  }

  @Test
  void filter_containsNonStringScore() {
    check("MATCH (c:Chunk) WHERE c.score CONTAINS '5' RETURN c.external_id AS id ORDER BY id",
        map(),
        new String[] { "id" });
  }

  @Test
  void filter_endsWithAccent() {
    check("MATCH (c:Chunk) WHERE c.section_heading ENDS WITH $s RETURN c.external_id AS id ORDER BY id",
        map("s", "umé"),
        new String[] { "id" },
        "c2");
  }

  @Test
  void filter_booleanFlagCoalesced() {
    check("MATCH (c:Chunk) WHERE coalesce(c.is_table, false) = $f RETURN c.external_id AS id ORDER BY id",
        map("f", false),
        new String[] { "id" },
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_booleanFlagPlain() {
    check("MATCH (c:Chunk) WHERE c.is_table = $f RETURN c.external_id AS id ORDER BY id",
        map("f", false),
        new String[] { "id" },
        "c3");
  }

  @Test
  void filter_flagNullParam() {
    check("MATCH (c:Chunk) WHERE c.is_table = $f RETURN c.external_id AS id ORDER BY id",
        map("f", null),
        new String[] { "id" });
  }

  @Test
  void filter_principalsDiffer() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE NOT p IN $t]) > 0 OR size([p IN $t WHERE NOT p IN coalesce(c.access_principals, [])]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1", "p2")),
        new String[] { "id" },
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_principalsDifferEmptyTokens() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE NOT p IN $t]) > 0 OR size([p IN $t WHERE NOT p IN coalesce(c.access_principals, [])]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list()),
        new String[] { "id" },
        "c1",
        "c3");
  }

  @Test
  void filter_principalsNullToken() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE NOT p IN $t]) > 0 OR size([p IN $t WHERE NOT p IN coalesce(c.access_principals, [])]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1", null)),
        new String[] { "id" },
        "c2",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void filter_principalsIntersect() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE p IN $t]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1")),
        new String[] { "id" },
        "c1",
        "c3");
  }

  @Test
  void filter_principalsIntersectOther() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE p IN $t]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list("p3", null)),
        new String[] { "id" },
        "c3");
  }

  @Test
  void filter_principalsIntersectNullOnly() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE p IN $t]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list(new Object[] { null })),
        new String[] { "id" });
  }

  @Test
  void filter_principalsIntersectEmpty() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE p IN $t]) > 0 RETURN c.external_id AS id ORDER BY id",
        map("t", list()),
        new String[] { "id" });
  }

  @Test
  void filter_principalsIntersectAndScope() {
    check("MATCH (c:Chunk) WHERE size([p IN coalesce(c.access_principals, []) WHERE p IN $t]) > 0 AND coalesce(c.graph_name, '') IN $g RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1", "p3"), "g", list("", "g1")),
        new String[] { "id" },
        "c1");
  }

  @Test
  void filter_principalsAny() {
    check("MATCH (c:Chunk) WHERE any(p IN c.access_principals WHERE p IN $t) RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1")),
        new String[] { "id" },
        "c1",
        "c3");
  }

  @Test
  void filter_principalsAll() {
    check("MATCH (c:Chunk) WHERE all(p IN c.access_principals WHERE p IN $t) RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1", "p2")),
        new String[] { "id" },
        "c1",
        "c2");
  }

  @Test
  void filter_principalsNone() {
    check("MATCH (c:Chunk) WHERE none(p IN c.access_principals WHERE p IN $t) RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1")),
        new String[] { "id" },
        "c2");
  }

  @Test
  void filter_principalsSingle() {
    check("MATCH (c:Chunk) WHERE single(p IN c.access_principals WHERE p IN $t) RETURN c.external_id AS id ORDER BY id",
        map("t", list("p1")),
        new String[] { "id" },
        "c1");
  }

  @Test
  void filter_principalsNullOrAny() {
    check("MATCH (c:Chunk) WHERE c.access_principals IS NULL OR any(p IN c.access_principals WHERE p IN $t) RETURN c.external_id AS id ORDER BY id",
        map("t", list("p3")),
        new String[] { "id" },
        "c3",
        "c4",
        "c5",
        "c6",
        "c7");
  }

  @Test
  void countGateMissingModel() {
    check("MATCH (c:Chunk) WHERE c.embedding IS NULL OR c.embedding_model <> $m RETURN count(c) AS n, count(c.embedding) AS ne",
        map("m", null),
        new String[] { "n", "ne" },
        "1,0");
  }

  @Test
  void countGateModel() {
    check("MATCH (c:Chunk) WHERE c.embedding IS NULL OR c.embedding_model <> $m RETURN count(c) AS n, count(c.embedding) AS ne",
        map("m", "m1"),
        new String[] { "n", "ne" },
        "2,1");
  }

  @Test
  void countNotDeleted() {
    check("MATCH (c:Chunk) WHERE c.deleted_at IS NULL RETURN count(c) AS n, count(*) AS s, count(c.embedding_model) AS m",
        map(),
        new String[] { "n", "s", "m" },
        "6,6,6");
  }

  @Test
  void countByNullModelProperty() {
    check("MATCH (c:Chunk {embedding_model: $m}) RETURN count(c) AS n",
        map("m", null),
        new String[] { "n" },
        "0");
  }

  @Test
  void countByModelProperty() {
    check("MATCH (c:Chunk {embedding_model: $m}) RETURN count(c) AS n",
        map("m", "m1"),
        new String[] { "n" },
        "5");
  }

  @Test
  void optionalAggregates() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.deleted_at IS NULL RETURN r.external_id AS rid, count(c) AS n, count(*) AS s, count(c.embedding) AS ne, sum(c.score) AS total, size(collect(DISTINCT c.external_id)) AS ids ORDER BY rid",
        map(),
        new String[] { "rid", "n", "s", "ne", "total", "ids" },
        "r1,2,2,1,3.5,2",
        "r2,2,2,2,2,2",
        "r3,1,1,1,1,1",
        "r4,1,1,1,0,1",
        "r5,0,1,0,0,0");
  }

  @Test
  void optionalAggregatesGated() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.embedding IS NULL OR c.embedding_model <> $m RETURN r.external_id AS rid, count(c) AS n, size(collect(DISTINCT c.external_id)) AS ids ORDER BY rid",
        map("m", "m1"),
        new String[] { "rid", "n", "ids" },
        "r1,1,1",
        "r2,1,1",
        "r3,0,0",
        "r4,0,0",
        "r5,0,0");
  }

  @Test
  void optionalHaving() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WITH r, count(c) AS n WHERE n = 0 RETURN r.external_id AS rid ORDER BY rid",
        map(),
        new String[] { "rid" },
        "r5");
  }

  @Test
  void optionalHavingAbsent() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WITH r, c WHERE c IS NULL RETURN r.external_id AS rid ORDER BY rid",
        map(),
        new String[] { "rid" },
        "r5");
  }

  @Test
  void optionalNullBoundNext() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.external_id = 'c5' OPTIONAL MATCH (c)-[:OF_RECORD]->(r2:Record) RETURN r.external_id AS rid, c.external_id AS cid, r2.external_id AS r2 ORDER BY rid",
        map(),
        new String[] { "rid", "cid", "r2" },
        "r1,null,null",
        "r2,null,null",
        "r3,null,null",
        "r4,c5,r4",
        "r5,null,null");
  }

  @Test
  void optionalNullBoundWhere() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.external_id = 'c5' OPTIONAL MATCH (r)<-[:OF_RECORD]-(d:Chunk) WHERE d.external_id <> c.external_id RETURN r.external_id AS rid, c.external_id AS cid, d.external_id AS did ORDER BY rid, did",
        map(),
        new String[] { "rid", "cid", "did" },
        "r1,null,null",
        "r2,null,null",
        "r3,null,null",
        "r4,c5,null",
        "r5,null,null");
  }

  @Test
  void optionalGateOnOptionalOnly() {
    check("MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WITH r, c WHERE c IS NULL OR c.deleted_at IS NULL RETURN r.external_id AS rid, c.external_id AS cid ORDER BY rid, cid",
        map(),
        new String[] { "rid", "cid" },
        "r1,c1",
        "r1,c2",
        "r2,c4",
        "r2,c7",
        "r3,c1",
        "r4,c5",
        "r5,null");
  }

  @Test
  void optionalOverEmptyUnwind() {
    check("UNWIND [] AS x OPTIONAL MATCH (c:Chunk {external_id: x}) RETURN x, c.external_id AS id",
        map(),
        new String[] { "x", "id" });
  }

  @Test
  void optionalOverEmptyMatch() {
    check("MATCH (r:Record {external_id: 'nope'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) RETURN r.external_id AS rid, c.external_id AS id",
        map(),
        new String[] { "rid", "id" });
  }

  @Test
  void optionalOverEmptyMatchCounted() {
    check("MATCH (r:Record {external_id: 'nope'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "0,0");
  }

  @Test
  void optionalOverEmptyWithNullParam() {
    check("UNWIND $anchor_ids AS a OPTIONAL MATCH (r:Record {external_id: a}) RETURN a, r.external_id AS rid",
        map("anchor_ids", list(new Object[] { null })),
        new String[] { "a", "rid" },
        "null,null");
  }

  @Test
  void optionalBatch_1() {
    check("UNWIND range(1, 1) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1,1,1,1");
  }

  @Test
  void optionalBatch_2() {
    check("UNWIND range(1, 2) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "2,2,3,2");
  }

  @Test
  void optionalBatch_99() {
    check("UNWIND range(1, 99) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "99,77,4950,7");
  }

  @Test
  void optionalBatch_100() {
    check("UNWIND range(1, 100) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "100,78,5050,7");
  }

  @Test
  void optionalBatchWhere_100() {
    check("UNWIND range(1, 100) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "200,200");
  }

  @Test
  void optionalBatch_101() {
    check("UNWIND range(1, 101) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "101,79,5151,7");
  }

  @Test
  void optionalBatchWhere_101() {
    check("UNWIND range(1, 101) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "202,202");
  }

  @Test
  void optionalBatch_199() {
    check("UNWIND range(1, 199) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "199,155,19900,7");
  }

  @Test
  void optionalBatch_200() {
    check("UNWIND range(1, 200) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "200,156,20100,7");
  }

  @Test
  void optionalBatch_201() {
    check("UNWIND range(1, 201) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "201,157,20301,7");
  }

  @Test
  void optionalBatch_255() {
    check("UNWIND range(1, 255) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "255,199,32640,7");
  }

  @Test
  void optionalBatch_256() {
    check("UNWIND range(1, 256) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "256,200,32896,7");
  }

  @Test
  void optionalBatch_257() {
    check("UNWIND range(1, 257) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "257,201,33153,7");
  }

  @Test
  void optionalBatch_999() {
    check("UNWIND range(1, 999) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "999,777,499500,7");
  }

  @Test
  void optionalBatch_1000() {
    check("UNWIND range(1, 1000) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1000,778,500500,7");
  }

  @Test
  void optionalBatchWhere_1000() {
    check("UNWIND range(1, 1000) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "2000,2000");
  }

  @Test
  void optionalBatch_1001() {
    check("UNWIND range(1, 1001) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1001,779,501501,7");
  }

  @Test
  void optionalBatchWhere_1001() {
    check("UNWIND range(1, 1001) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "2002,2002");
  }

  @Test
  void optionalBatch_1023() {
    check("UNWIND range(1, 1023) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1023,797,523776,7");
  }

  @Test
  void optionalBatch_1024() {
    check("UNWIND range(1, 1024) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1024,798,524800,7");
  }

  @Test
  void optionalBatch_1025() {
    check("UNWIND range(1, 1025) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "1025,798,525825,7");
  }

  @Test
  void optionalBatch_4096() {
    check("UNWIND range(1, 4096) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "4096,3186,8390656,7");
  }

  @Test
  void optionalBatch_4097() {
    check("UNWIND range(1, 4097) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "4097,3187,8394753,7");
  }

  @Test
  void optionalBatch_10000() {
    check("UNWIND range(1, 10000) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "10000,7778,50005000,7");
  }

  @Test
  void optionalBatchWhere_10000() {
    check("UNWIND range(1, 10000) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "20000,20000");
  }

  @Test
  void optionalBatch_10001() {
    check("UNWIND range(1, 10001) AS i OPTIONAL MATCH (c:Chunk {external_id: 'c' + toString(i % 9)}) RETURN count(*) AS n, count(c) AS cn, sum(i) AS si, count(DISTINCT c) AS dc",
        map(),
        new String[] { "n", "cn", "si", "dc" },
        "10001,7779,50015001,7");
  }

  @Test
  void optionalBatchWhere_10001() {
    check("UNWIND range(1, 10001) AS i MATCH (r:Record {external_id: 'r1'}) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) WHERE c.score >= i % 2 RETURN count(*) AS n, count(c) AS cn",
        map(),
        new String[] { "n", "cn" },
        "20002,20002");
  }
}
