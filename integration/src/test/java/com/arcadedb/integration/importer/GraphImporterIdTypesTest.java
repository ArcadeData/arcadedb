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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Identity keys that are not a positive {@code int}: string keys, keys wider than {@code int}, the
 * key {@code 0}, and keys whose textual form is not canonical (leading zeros). Every one of them
 * has to resolve the same way from a vertex source, from a foreign key carried on a vertex record
 * and from a standalone edge source (issue #7244, discussion #7214).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterIdTypesTest {

  private static final String DB_PATH   = "target/databases/graph-importer-id-types-test";
  private static final String DATA_PATH = "target/test-data/graph-importer-id-types";

  private Database database;
  private File     dataDir;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    dataDir = new File(DATA_PATH);
    dataDir.mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  private String write(final String fileName, final String... lines) throws Exception {
    final File f = new File(dataDir, fileName);
    Files.write(f.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }

  /**
   * The reporter's first case: JSONL vertices keyed by an OpenAlex-style string id, with the
   * topology in a separate edge file. Before #7244 the vertex pass alone threw
   * {@code JSONObject[id] is not a int}.
   */
  @Test
  void stringVertexIdsResolveFromAStandaloneEdgeSource() throws Exception {
    final String vertices = write("string-vertices.jsonl",
        "{\"id\": \"W13696992\", \"title\": \"Alpha\", \"publication_year\": 2008}",
        "{\"id\": \"W28031933\", \"title\": \"Beta\", \"publication_year\": 2011}",
        "{\"id\": \"W99999999\", \"title\": \"Gamma\", \"publication_year\": 2014}");
    final String edges = write("string-edges.csv",
        "from_id,to_id",
        "W13696992,W28031933",
        "W13696992,W99999999");

    database.transaction(() -> {
      database.getSchema().createVertexType("Document");
      database.getSchema().createEdgeType("Cites");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Document", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("id", "id");
          v.property("title", "title");
          v.intProperty("publicationYear", "publication_year");
        })
        .edgeSource("Cites", new CsvRowSource(edges), e -> {
          e.from("from_id", "Document");
          e.to("to_id", "Document");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(3);
      assertThat(importer.getEdgeCount()).isEqualTo(2);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Document", "id", "W13696992", "Cites", "id"))
        .containsExactlyInAnyOrder("W28031933", "W99999999");
  }

  /**
   * The reporter's second case: ids that fit a {@code long} but not an {@code int}. Before #7244
   * the edge source failed with {@code NumberFormatException: For input string: "2257721487"}.
   */
  @Test
  void longVertexIdsResolveFromAStandaloneEdgeSource() throws Exception {
    final String vertices = write("long-vertices.csv",
        "Id,Name",
        "2257721487,Alpha",
        "28031933,Beta",
        "2805857155,Gamma",
        "1643034221,Delta",
        "2186349252,Epsilon");
    final String edges = write("long-edges.csv",
        "from_id,to_id",
        "2257721487,28031933",
        "2186349252,2805857155",
        "2186349252,1643034221");

    database.transaction(() -> {
      database.getSchema().createVertexType("Doc");
      database.getSchema().createEdgeType("Connection");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Doc", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.longProperty("docId", "Id");
          v.property("name", "Name");
        })
        .edgeSource("Connection", new CsvRowSource(edges), e -> {
          e.from("from_id", "Doc");
          e.to("to_id", "Doc");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(5);
      assertThat(importer.getEdgeCount()).isEqualTo(3);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Doc", "docId", 2186349252L, "Connection", "name"))
        .containsExactlyInAnyOrder("Gamma", "Delta");
  }

  /**
   * {@code 0} is a legitimate key. It used to be indistinguishable from "no foreign key here",
   * because the reader turned a missing attribute into {@code 0} and the collector read {@code 0}
   * as the absent marker.
   */
  @Test
  void vertexIdZeroIsAValidEdgeEndpoint() throws Exception {
    final String vertices = write("zero-vertices.csv",
        "Id,Name",
        "0,Root",
        "1,Child",
        "2,Orphan");
    final String edges = write("zero-edges.csv",
        "from_id,to_id",
        "1,0");

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("Parent");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Node", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.intProperty("nodeId", "Id");
          v.property("name", "Name");
        })
        .edgeSource("Parent", new CsvRowSource(edges), e -> {
          e.from("from_id", "Node");
          e.to("to_id", "Node");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(1);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Node", "nodeId", 1, "Parent", "name")).containsExactly("Root");
  }

  /**
   * A foreign key carried on the vertex record, pointing at a vertex whose id is {@code 0}: the
   * same absent-versus-zero confusion, on the {@code edgeOut} path.
   */
  @Test
  void vertexRecordForeignKeyToIdZeroIsResolved() throws Exception {
    final String departments = write("zero-departments.csv",
        "Id,Name",
        "0,Engineering",
        "1,Sales");
    final String employees = write("zero-employees.csv",
        "Id,Name,DeptId",
        "10,Alice,0",
        "11,Bob,1",
        "12,Carol,");

    database.transaction(() -> {
      database.getSchema().createVertexType("Department");
      database.getSchema().createVertexType("Employee");
      database.getSchema().createEdgeType("WORKS_IN");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Department", new CsvRowSource(departments), v -> {
          v.id("Id");
          v.property("name", "Name");
        })
        .vertex("Employee", new CsvRowSource(employees), v -> {
          v.id("Id");
          v.intProperty("empId", "Id");
          v.property("name", "Name");
          v.edgeOut("DeptId", "WORKS_IN", "Department");
        })
        .build()) {

      importer.run();

      // Carol has no department: an empty attribute is still "no edge"
      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    assertThat(outgoingTargets("Employee", "empId", 10, "WORKS_IN", "name")).containsExactly("Engineering");
    assertThat(outgoingTargets("Employee", "empId", 12, "WORKS_IN", "name")).isEmpty();
  }

  /**
   * A self-referencing edge resolved by name. The deferred path that handles {@code targetType ==
   * thisType} read the attribute as an {@code int} whatever the edge declared, so declaring it
   * {@code byName} threw on the first row.
   */
  @Test
  void selfReferencingEdgeResolvesByName() throws Exception {
    final String vertices = write("tree.csv",
        "Code,Parent",
        "root,",
        "child-a,root",
        "child-b,root",
        "grandchild,child-a");

    database.transaction(() -> {
      database.getSchema().createVertexType("Category");
      database.getSchema().createEdgeType("ChildOf");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Category", new CsvRowSource(vertices), v -> {
          v.idByName("Code");
          v.property("code", "Code");
          v.edgeOutByName("Parent", "ChildOf", "Category");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(4);
      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    assertThat(outgoingTargets("Category", "code", "grandchild", "ChildOf", "code")).containsExactly("child-a");
    assertThat(outgoingTargets("Category", "code", "root", "ChildOf", "code")).isEmpty();
  }

  /**
   * A self-referencing edge on a string primary id: the deferred path has to widen with the index
   * rather than assume the key is numeric.
   */
  @Test
  void selfReferencingEdgeResolvesOnStringPrimaryId() throws Exception {
    final String vertices = write("string-tree.csv",
        "Id,Parent",
        "W1,",
        "W2,W1",
        "W3,W2");

    database.transaction(() -> {
      database.getSchema().createVertexType("Doc");
      database.getSchema().createEdgeType("AnswerOf");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Doc", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.property("docId", "Id");
          v.edgeOut("Parent", "AnswerOf", "Doc");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    assertThat(outgoingTargets("Doc", "docId", "W3", "AnswerOf", "docId")).containsExactly("W2");
  }

  /**
   * A key whose text is not the canonical form of its numeric value stays a distinct key:
   * {@code "007"} and {@code "7"} are two vertices, and an edge naming one must not reach the
   * other. Parsing both as {@code 7} would silently join unrelated rows.
   */
  @Test
  void nonCanonicalNumericKeysDoNotCollide() throws Exception {
    final String vertices = write("zip-vertices.csv",
        "Zip,Name",
        "007,Bond",
        "7,Seven",
        "12,Twelve");
    final String edges = write("zip-edges.csv",
        "from_id,to_id",
        "12,007");

    database.transaction(() -> {
      database.getSchema().createVertexType("Area");
      database.getSchema().createEdgeType("Near");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Area", new CsvRowSource(vertices), v -> {
          v.id("Zip");
          v.property("zip", "Zip");
          v.property("name", "Name");
        })
        .edgeSource("Near", new CsvRowSource(edges), e -> {
          e.from("from_id", "Area");
          e.to("to_id", "Area");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(3);
      assertThat(importer.getEdgeCount()).isEqualTo(1);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Area", "zip", "12", "Near", "name")).containsExactly("Bond");
  }

  /**
   * Numeric and non-numeric keys in the same column. The index starts on the primitive fast path
   * and widens when it meets a key that cannot live there; keys registered before the widening
   * still resolve afterwards, and keys wider than {@code int} do not truncate.
   */
  @Test
  void mixedKeyWidthsAndTypesAllResolve() throws Exception {
    final String vertices = write("mixed-vertices.csv",
        "Id,Name",
        "1,One",
        "2257721487,Wide",
        "W3,Text",
        "-5,Negative");
    final String edges = write("mixed-edges.csv",
        "from_id,to_id",
        "1,2257721487",
        "1,W3",
        "1,-5",
        "W3,1");

    database.transaction(() -> {
      database.getSchema().createVertexType("Thing");
      database.getSchema().createEdgeType("Link");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Thing", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.property("tid", "Id");
          v.property("name", "Name");
        })
        .edgeSource("Link", new CsvRowSource(edges), e -> {
          e.from("from_id", "Thing");
          e.to("to_id", "Thing");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(4);
      assertThat(importer.getEdgeCount()).isEqualTo(4);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Thing", "tid", "1", "Link", "name"))
        .containsExactlyInAnyOrder("Wide", "Text", "Negative");
    assertThat(outgoingTargets("Thing", "tid", "W3", "Link", "name")).containsExactly("One");
  }

  /**
   * A self-referencing edge declared {@code edgeIn}: the foreign key names the vertex that points
   * TO this row, so the edge runs parent to child. The deferred path used to write this row as the
   * source whatever the direction said, building every such edge backwards without failing.
   */
  @Test
  void selfReferencingEdgeInRunsFromTheReferencedVertex() throws Exception {
    final String vertices = write("parent-tree.csv",
        "Id,ParentId",
        "1,",
        "2,1",
        "3,1",
        "4,2");

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("HasChild");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Node", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.intProperty("nodeId", "Id");
          v.edgeIn("ParentId", "HasChild", "Node");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    // 1 -> 2, 1 -> 3, 2 -> 4: the parent is the source, never the row that named it
    assertThat(outgoingProperties("Node", "nodeId", 1, "HasChild", "nodeId"))
        .containsExactlyInAnyOrder(2, 3);
    assertThat(outgoingProperties("Node", "nodeId", 2, "HasChild", "nodeId")).containsExactly(4);
    assertThat(outgoingProperties("Node", "nodeId", 4, "HasChild", "nodeId")).isEmpty();
  }

  /**
   * An empty identity is no identity. CSV reports an empty field as absent, but JSONL and XML hand
   * back the empty string, so without normalising it two rows with {@code "id": ""} would collide
   * on a key of their own and an edge naming it would reach whichever of them registered last.
   */
  @Test
  void anEmptyIdentityRegistersNoKey() throws Exception {
    final String vertices = write("empty-id-vertices.jsonl",
        "{\"id\": \"\", \"name\": \"NoKeyOne\"}",
        "{\"id\": \"\", \"name\": \"NoKeyTwo\"}",
        "{\"id\": \"W1\", \"name\": \"Real\"}");
    final String edges = write("empty-id-edges.csv",
        "from_id,to_id",
        "W1,",
        ",W1");

    database.transaction(() -> {
      database.getSchema().createVertexType("Document");
      database.getSchema().createEdgeType("Cites");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Document", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("id", "id");
          v.property("name", "name");
        })
        .edgeSource("Cites", new CsvRowSource(edges), e -> {
          e.from("from_id", "Document");
          e.to("to_id", "Document");
        })
        .build()) {

      importer.run();

      // all three rows are vertices; neither endpoint naming an empty key resolves to one of them
      assertThat(importer.getVertexCount()).isEqualTo(3);
      assertThat(importer.getEdgeCount()).isZero();
      assertThat(importer.getUnresolvedEdgeCount()).isEqualTo(2);
    }
  }

  /**
   * A split field pointing at the type it lives on. Resolving it while the file is still being read
   * kept only the references that happened to point at an earlier row, so a forward reference was
   * dropped without a word.
   */
  @Test
  void selfReferencingSplitEdgeSeesForwardReferences() throws Exception {
    final String vertices = write("split-tree.csv",
        "Code,Related",
        "a,|b|c|",
        "b,|c|",
        "c,");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createEdgeType("RelatedTo");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(vertices), v -> {
          v.idByName("Code");
          v.property("code", "Code");
          v.splitEdge("Related", "RelatedTo", "Topic", "|");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(3);
      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    assertThat(outgoingTargets("Topic", "code", "a", "RelatedTo", "code"))
        .containsExactlyInAnyOrder("b", "c");
    assertThat(outgoingTargets("Topic", "code", "b", "RelatedTo", "code")).containsExactly("c");
  }

  /**
   * An endpoint that matches no vertex is still skipped, but it is now counted and reported
   * instead of quietly shrinking the graph.
   */
  @Test
  void unresolvedEndpointsAreCountedNotSilentlyDropped() throws Exception {
    final String vertices = write("small-vertices.csv",
        "Id,Name",
        "1,One",
        "2,Two");
    final String edges = write("dangling-edges.csv",
        "from_id,to_id",
        "1,2",
        "1,404",
        "999,2");

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("Knows");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Node", new CsvRowSource(vertices), v -> {
          v.id("Id");
          v.intProperty("nodeId", "Id");
          v.property("name", "Name");
        })
        .edgeSource("Knows", new CsvRowSource(edges), e -> {
          e.from("from_id", "Node");
          e.to("to_id", "Node");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(1);
      assertThat(importer.getUnresolvedEdgeCount()).isEqualTo(2);
    }
  }

  /**
   * Deduplication keys off the same identity rules, so a repeated string key is still one vertex.
   */
  @Test
  void deduplicationWorksOnStringIds() throws Exception {
    final String vertices = write("dup-vertices.jsonl",
        "{\"id\": \"W1\", \"title\": \"First\"}",
        "{\"id\": \"W1\", \"title\": \"Duplicate\"}",
        "{\"id\": \"W2\", \"title\": \"Second\"}");

    database.transaction(() -> database.getSchema().createVertexType("Document"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Document", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.deduplicate(true);
          v.property("id", "id");
          v.property("title", "title");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Document WHERE id = 'W1'")) {
        assertThat(rs.next().<String>getProperty("title")).isEqualTo("First");
      }
    });
  }

  private List<Integer> outgoingProperties(final String vertexType, final String keyProperty, final Object key,
                                           final String edgeType, final String targetProperty) {
    final List<Integer> result = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM " + vertexType + " WHERE " + keyProperty + " = ?", key)) {
        assertThat(rs.hasNext()).as("vertex %s.%s = %s", vertexType, keyProperty, key).isTrue();
        final Vertex v = rs.next().getVertex().get();
        for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT, edgeType))
          result.add(e.getInVertex().asVertex().getInteger(targetProperty));
      }
    });
    return result;
  }

  private List<String> outgoingTargets(final String vertexType, final String keyProperty, final Object key,
                                       final String edgeType, final String targetProperty) {
    final List<String> result = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM " + vertexType + " WHERE " + keyProperty + " = ?", key)) {
        assertThat(rs.hasNext()).as("vertex %s.%s = %s", vertexType, keyProperty, key).isTrue();
        final Vertex v = rs.next().getVertex().get();
        for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT, edgeType))
          result.add(e.getInVertex().asVertex().getString(targetProperty));
      }
    });
    return result;
  }
}
