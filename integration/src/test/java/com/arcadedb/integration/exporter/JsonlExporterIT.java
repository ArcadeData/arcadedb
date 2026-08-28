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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.IterableGraph;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonlExporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-export.jsonl.tgz";

  private Database emptyDatabase() {
    return new DatabaseFactory(DATABASE_PATH).create();
  }

  @BeforeEach
  @AfterEach
  void beforeTests() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void exportOK() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    final File file = new File(FILE);

    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();
    assertThat(databaseDirectory.exists()).isTrue();

    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    assertThat(file.exists()).isTrue();
    assertThat(file.length() > 0).isTrue();

    int lines = 0;
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
      while (in.ready()) {
        final String line = in.readLine();
        new JSONObject(line);
        ++lines;
      }
    }

    assertThat(lines > 10).isTrue();

  }

  @Test
  void formatError() {
    assertThatThrownBy(() -> {
      emptyDatabase().close();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format unknown").split(" ")).exportDatabase();
    }).isInstanceOf(ExportException.class);
  }

  @Test
  void fileCannotBeOverwrittenError() throws Exception {
    assertThatThrownBy(() -> {
      emptyDatabase().close();
      new File(FILE).createNewFile();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -format jsonl").split(" ")).exportDatabase();
    }).isInstanceOf(ExportException.class);
  }

  /**
   * Test for issue #1540: unique field is missing from the exported JSONL
   * <p>
   * When exporting a database to JSONL format, the schema indexes should include the "unique" field.
   */
  @Test
  void exportedIndexesContainUniqueField() throws Exception {
    final File file = new File(FILE);

    // Create a database with indexes (both unique and non-unique)
    try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Person");
        type.createProperty("name", String.class);
        type.createProperty("age", Integer.class);
        type.createProperty("email", String.class);

        // Create a non-unique index on age
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age");

        // Create a unique index on email
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "email");
      });
    }

    // Export the database
    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    assertThat(file.exists()).isTrue();

    // Read the exported file and verify the schema line contains "unique" field for indexes
    JSONObject schemaLine = null;
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
      while (in.ready()) {
        final String line = in.readLine();
        final JSONObject json = new JSONObject(line);
        if ("schema".equals(json.getString("t"))) {
          schemaLine = json.getJSONObject("c");
          break;
        }
      }
    }

    assertThat(schemaLine).as("Schema line not found in export").isNotNull();

    // Navigate to the Person type indexes
    final JSONObject types = schemaLine.getJSONObject("types");
    assertThat(types.has("Person")).as("Person type should exist in exported schema").isTrue();

    final JSONObject personType = types.getJSONObject("Person");
    final JSONObject indexes = personType.getJSONObject("indexes");
    assertThat(indexes.length()).as("Person type should have 2 indexes exported").isGreaterThanOrEqualTo(2);

    // Verify that each index has the "unique" field
    boolean foundNonUniqueIndex = false;
    boolean foundUniqueIndex = false;

    for (final String indexName : indexes.keySet()) {
      final JSONObject indexJson = indexes.getJSONObject(indexName);

      // Verify the "unique" field exists (issue #1540)
      assertThat(indexJson.has("unique"))
          .as("Index '%s' should have 'unique' field in exported JSONL (issue #1540)", indexName)
          .isTrue();

      // Also verify the value is correctly exported
      final boolean isUnique = indexJson.getBoolean("unique");
      final String properties = indexJson.getJSONArray("properties").toString();

      if (properties.contains("age")) {
        assertThat(isUnique).as("Index on 'age' should be non-unique").isFalse();
        foundNonUniqueIndex = true;
      } else if (properties.contains("email")) {
        assertThat(isUnique).as("Index on 'email' should be unique").isTrue();
        foundUniqueIndex = true;
      }
    }

    assertThat(foundNonUniqueIndex).as("Should find the non-unique index on age").isTrue();
    assertThat(foundUniqueIndex).as("Should find the unique index on email").isTrue();
  }

  /**
   * Issue #6471: a record that throws while being serialized must not make the export look complete. The
   * per-record catch in {@code JsonlExporterFormat} still skips the broken record and keeps going (that part
   * is deliberate, issue #6471's own description), but the export as a whole must now report failure and the
   * skipped count, instead of silently printing its counters as if nothing were missing.
   */
  @Test
  void exportFailsLoudlyWhenARecordCannotBeSerialized() throws Exception {
    final File file = new File(FILE);

    try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createVertexType("Widget");
        type.createProperty("name", String.class);
      });
      db.transaction(() -> {
        db.newVertex("Widget").set("name", "first").save();
        db.newVertex("Widget").set("name", "second").save();
      });
    }

    final DatabaseInternal realDatabase = (DatabaseInternal) new DatabaseFactory(DATABASE_PATH).open();
    try {
      // A JDK dynamic proxy that forwards every call to the real, already-open database, except
      // iterateType("Widget", false): that call still returns a real iterator over the real records, but the
      // first record it hands back throws once (simulating a record that fails to serialize, e.g. #6471's own
      // DATE-unit example) before behaving normally for the rest. Forwarding through method.invoke() keeps every
      // internal self-call running with the REAL database as "this", which matters here: the engine keys its
      // per-thread transaction context by database identity (LocalDatabase#getTransactionIfExists), so a copied
      // instance (e.g. a Mockito spy(), which clones state onto a new object) is rejected as "a different db".
      final DatabaseInternal proxyDatabase = (DatabaseInternal) Proxy.newProxyInstance(
          DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
          (proxy, method, args) -> {
            if ("iterateType".equals(method.getName()) && "Widget".equals(args[0]) && Boolean.FALSE.equals(args[1])) {
              final Iterator<Record> real = (Iterator<Record>) method.invoke(realDatabase, args);
              return new Iterator<Record>() {
                private boolean thrown = false;

                @Override
                public boolean hasNext() {
                  return real.hasNext();
                }

                @Override
                public Record next() {
                  final Record record = real.next();
                  if (!thrown) {
                    thrown = true;
                    throw new RuntimeException("Simulated export serialization failure");
                  }
                  return record;
                }
              };
            }
            try {
              return method.invoke(realDatabase, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });

      final Exporter exporter = new Exporter(proxyDatabase, FILE);
      exporter.setFormat("jsonl").setOverwrite(true);

      assertThatThrownBy(exporter::exportDatabase)//
          .isInstanceOf(ExportException.class)//
          .hasMessageContaining("skipped");

      // Best-effort: the file is still written with whatever did serialize successfully.
      assertThat(file.exists()).isTrue();

      int vertexLines = 0;
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
        while (in.ready()) {
          final String line = in.readLine();
          final JSONObject json = new JSONObject(line);
          if ("v".equals(json.getString("t")))
            ++vertexLines;
        }
      }
      // Two vertex-type iterations touch "Widget" (exportVertices and exportLightweightEdges), each one skips
      // its own first record: only the second, unaffected widget makes it into exportVertices' output.
      assertThat(vertexLines).isEqualTo(1);

    } finally {
      if (realDatabase.isOpen())
        realDatabase.close();
    }
  }

  /**
   * Issue #6471 follow-up (#6795, comment on the closed issue): {@code exportLightweightEdges} wrapped a whole
   * vertex's per-edge loop in ONE try/catch, so a failure on any single edge counted {@code skippedRecords} +1
   * and silently dropped the REST of that vertex's remaining edges too - the same silent-drop class #6471
   * targeted, surviving inside the very file the fix landed in. The primary guarantee still held (any skip still
   * fails the export loudly via {@code ExportException}), so this pins the finer-grained guarantee: a failure on
   * one edge must not cost its siblings.
   */
  @Test
  void exportLightweightEdgesContinuesAfterOneEdgeFailsToSerialize() throws Exception {
    final File file = new File(FILE);

    final RID hubRid;
    final RID leafARid;
    final RID leafBRid;
    try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
      db.transaction(() -> {
        db.getSchema().buildVertexType().withName("Node").create();
        db.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create();
      });

      final MutableVertex[] holder = new MutableVertex[3];
      db.transaction(() -> {
        holder[0] = db.newVertex("Node").set("id", 0).save();
        holder[1] = db.newVertex("Node").set("id", 1).save();
        holder[2] = db.newVertex("Node").set("id", 2).save();
        holder[0].newEdge("Follows", holder[1]);
        holder[0].newEdge("Follows", holder[2]);
      });

      hubRid = holder[0].getIdentity();
      leafARid = holder[1].getIdentity();
      leafBRid = holder[2].getIdentity();
    }

    final DatabaseInternal realDatabase = (DatabaseInternal) new DatabaseFactory(DATABASE_PATH).open();
    try {
      final Vertex realHub = (Vertex) realDatabase.lookupByRID(hubRid, true);
      Edge realEdgeToA = null;
      Edge realEdgeToB = null;
      for (final Edge e : realHub.getEdges(Vertex.DIRECTION.OUT)) {
        if (leafARid.equals(e.getIn().getIdentity()))
          realEdgeToA = e;
        else if (leafBRid.equals(e.getIn().getIdentity()))
          realEdgeToB = e;
      }
      assertThat(realEdgeToA).isNotNull();
      assertThat(realEdgeToB).isNotNull();

      // The edge to leaf A always throws when serialized; the edge to leaf B must still make it into the export.
      final Edge finalRealEdgeToA = realEdgeToA;
      final Edge failingEdgeToA = (Edge) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { LightEdge.class },
          (proxy, method, args) -> {
            if ("toMap".equals(method.getName()))
              throw new RuntimeException("Simulated edge serialization failure");
            try {
              return method.invoke(finalRealEdgeToA, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });

      final List<Edge> craftedOutEdges = List.of(failingEdgeToA, realEdgeToB);
      final IterableGraph<Edge> craftedIterable = new IterableGraph<>() {
        @Override
        public Class<? extends Document> getEntryType() {
          return Edge.class;
        }

        @Override
        public Iterator<Edge> iterator() {
          return craftedOutEdges.iterator();
        }
      };

      final Vertex hubVertexProxy = (Vertex) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Vertex.class },
          (proxy, method, args) -> {
            if ("getEdges".equals(method.getName()) && args != null && args.length >= 1 && Vertex.DIRECTION.OUT.equals(args[0]))
              return craftedIterable;
            try {
              return method.invoke(realHub, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });

      // Same proxying idiom as exportFailsLoudlyWhenARecordCannotBeSerialized above: every call forwards to the
      // real database, except iterateType("Node", false), whose returned records are passed through unchanged -
      // EXCEPT the hub, whose asVertex(true) hands back hubVertexProxy instead.
      final DatabaseInternal proxyDatabase = (DatabaseInternal) Proxy.newProxyInstance(
          DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
          (proxy, method, args) -> {
            if ("iterateType".equals(method.getName()) && "Node".equals(args[0]) && Boolean.FALSE.equals(args[1])) {
              final Iterator<Record> real = (Iterator<Record>) method.invoke(realDatabase, args);
              return new Iterator<Record>() {
                @Override
                public boolean hasNext() {
                  return real.hasNext();
                }

                @Override
                public Record next() {
                  final Record record = real.next();
                  if (!hubRid.equals(record.getIdentity()))
                    return record;

                  return (Record) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Record.class },
                      (recordProxy, recordMethod, recordArgs) -> {
                        if ("asVertex".equals(recordMethod.getName()))
                          return hubVertexProxy;
                        try {
                          return recordMethod.invoke(record, recordArgs);
                        } catch (final InvocationTargetException e) {
                          throw e.getCause();
                        }
                      });
                }
              };
            }
            try {
              return method.invoke(realDatabase, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });

      final Exporter exporter = new Exporter(proxyDatabase, FILE);
      exporter.setFormat("jsonl").setOverwrite(true);

      assertThatThrownBy(exporter::exportDatabase)//
          .isInstanceOf(ExportException.class)//
          .hasMessageContaining("skipped");

      boolean foundEdgeToLeafB = false;
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
        while (in.ready()) {
          final String line = in.readLine();
          final JSONObject json = new JSONObject(line);
          if ("e".equals(json.getString("t")) && leafBRid.toString().equals(json.getJSONObject("c").getString("i")))
            foundEdgeToLeafB = true;
        }
      }
      assertThat(foundEdgeToLeafB)
          .as("the edge to the SECOND leaf must still be exported despite the first edge's failure")
          .isTrue();
    } finally {
      if (realDatabase.isOpen())
        realDatabase.close();
    }
  }

}
