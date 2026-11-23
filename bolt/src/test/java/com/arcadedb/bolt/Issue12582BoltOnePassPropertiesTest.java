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
package com.arcadedb.bolt;

import com.arcadedb.bolt.structure.BoltNode;
import com.arcadedb.bolt.structure.BoltRelationship;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.bolt.structure.BoltUnboundRelationship;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.ImmutableEdge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Bolt serializer reads a node or a relationship in one pass over its record (issue #12582).
 * <p>
 * It asked the element for every property by name, and for a stored record each of those reads walks the record header from
 * its first byte, so an element of P properties cost P walks and the time per element grew with P squared: the defect #11738
 * and #12558 fixed on the HTTP, gRPC and Postgres paths. The counting records below make the walks visible.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12582BoltOnePassPropertiesTest {
  private static final String DB_PATH    = "./target/databases/Issue12582BoltOnePassPropertiesTest";
  private static final int    PROPERTIES = 40;

  private Database db;
  private RID      vertexRid;
  private RID      edgeRid;
  private RID      documentRid;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.getSchema().createVertexType("Wide");
    db.getSchema().createEdgeType("WideEdge");
    db.getSchema().createDocumentType("WideDoc");
    db.transaction(() -> {
      final MutableVertex a = db.newVertex("Wide");
      final MutableVertex b = db.newVertex("Wide");
      for (int i = 0; i < PROPERTIES; i++) {
        a.set("p" + i, "value-" + i);
        b.set("p" + i, "value-" + i);
      }
      a.save();
      b.save();
      vertexRid = a.getIdentity();

      final MutableEdge edge = a.newEdge("WideEdge", b);
      for (int i = 0; i < PROPERTIES; i++)
        edge.set("p" + i, "value-" + i);
      edgeRid = edge.save().getIdentity();

      final MutableDocument doc = db.newDocument("WideDoc");
      for (int i = 0; i < PROPERTIES; i++)
        doc.set("p" + i, "value-" + i);
      documentRid = doc.save().getIdentity();
    });
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  // Regression: #12582
  @Test
  void aNodeIsReadOnceNotOncePerProperty() {
    db.transaction(() -> {
      final Counter counter = new Counter();
      final CountingVertex vertex = new CountingVertex(db, type("Wide"), vertexRid, counter);

      final BoltNode node = BoltStructureMapper.toNode(vertex);

      assertAllProperties(node.getProperties());
      assertOnePass(counter);
    });
  }

  // Regression: #12582
  @Test
  void aRelationshipIsReadOnceNotOncePerProperty() {
    db.transaction(() -> {
      final Counter counter = new Counter();
      final BoltRelationship relationship = BoltStructureMapper.toRelationship(new CountingEdge(db, type("WideEdge"), edgeRid, counter));

      assertAllProperties(relationship.getProperties());
      assertOnePass(counter);

      final Counter unboundCounter = new Counter();
      final BoltUnboundRelationship unbound = BoltStructureMapper.toUnboundRelationship(
          new CountingEdge(db, type("WideEdge"), edgeRid, unboundCounter));

      assertAllProperties(unbound.getProperties());
      assertOnePass(unboundCounter);
    });
  }

  // Regression: #12582
  @Test
  void aDocumentAndARowOfARecordAreReadOnceNotOncePerProperty() {
    db.transaction(() -> {
      final Counter counter = new Counter();
      final Object asMap = BoltStructureMapper.toPackStreamValue(new CountingDocument(db, type("WideDoc"), documentRid, counter));
      assertAllProperties(asMap(asMap));
      assertOnePass(counter);

      final Counter rowCounter = new Counter();
      final Object fromRow = BoltStructureMapper.toPackStreamValue(
          new ResultInternal(new CountingDocument(db, type("WideDoc"), documentRid, rowCounter)));
      assertAllProperties(asMap(fromRow));
      assertOnePass(rowCounter);
    });
  }

  // Regression: #12582
  @Test
  void theFieldsOfARecordRowAreReadOnceNotOncePerField() {
    db.transaction(() -> {
      final Counter counter = new Counter();
      // a row backed by a record and no projection name: the executor sends one value per field the first row announced
      final ResultInternal row = new ResultInternal(new CountingVertex(db, type("Wide"), vertexRid, counter));
      final List<String> fields = new ArrayList<>(row.getPropertyNames());
      counter.reset();

      final List<Object> values = BoltNetworkExecutor.extractRecordValues(row, fields);

      assertThat(values).hasSize(PROPERTIES);
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(values.get(fields.indexOf("p" + i))).as("field p%d", i).isEqualTo("value-" + i);
      assertOnePass(counter);
    });
  }

  // A projection row holds its values: they are answered as they are, in the order the row lists them
  @Test
  void aProjectionRowKeepsItsValuesAndOrder() {
    final ResultInternal row = new ResultInternal();
    row.setProperty("b", 2L);
    row.setProperty("a", "x");
    row.setProperty("c", null);

    final List<Object> values = BoltNetworkExecutor.extractRecordValues(row, List.of("a", "b", "c", "missing"));
    assertThat(values).containsExactly("x", 2L, null, null);

    final Map<String, Object> asMap = asMap(BoltStructureMapper.toPackStreamValue(row));
    assertThat(asMap.keySet()).containsExactly("b", "a", "c");
    assertThat(asMap).containsEntry("b", 2L).containsEntry("a", "x").containsEntry("c", null);
  }

  private DocumentType type(final String name) {
    return db.getSchema().getType(name);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(final Object value) {
    assertThat(value).isInstanceOf(Map.class);
    return (Map<String, Object>) value;
  }

  private static void assertAllProperties(final Map<String, Object> properties) {
    assertThat(properties).hasSize(PROPERTIES);
    for (int i = 0; i < PROPERTIES; i++)
      assertThat(properties).as("property p%d", i).containsEntry("p" + i, "value-" + i);
    // the order the record holds the properties in, as the reads by name kept it
    final List<String> names = new ArrayList<>(properties.keySet());
    for (int i = 0; i < PROPERTIES; i++)
      assertThat(names.get(i)).isEqualTo("p" + i);
  }

  private static void assertOnePass(final Counter counter) {
    assertThat(counter.gets).as("reads by name, each a walk over the record header").isZero();
    assertThat(counter.headerWalks()).as("passes over the record header, once for the element and not once per property")
        .isLessThanOrEqualTo(2);
  }

  /** Counts the reads that walk a record header from its first byte. */
  private static final class Counter {
    int gets;
    int names;
    int maps;

    int headerWalks() {
      return gets + names + maps;
    }

    void reset() {
      gets = 0;
      names = 0;
      maps = 0;
    }
  }

  private static final class CountingDocument extends ImmutableDocument {
    private final Counter counter;

    CountingDocument(final Database database, final DocumentType type, final RID rid, final Counter counter) {
      super(database, type, rid, null);
      this.counter = counter;
    }

    @Override
    public Object get(final String propertyName) {
      ++counter.gets;
      return super.get(propertyName);
    }

    @Override
    public Object getIfPresent(final String propertyName, final Object absentValue) {
      ++counter.gets;
      return super.getIfPresent(propertyName, absentValue);
    }

    @Override
    public Set<String> getPropertyNames() {
      ++counter.names;
      return super.getPropertyNames();
    }

    @Override
    public Map<String, Object> toMap(final boolean includeMetadata) {
      ++counter.maps;
      return super.toMap(includeMetadata);
    }

    @Override
    public Map<String, Object> propertiesAsMap() {
      ++counter.maps;
      return super.propertiesAsMap();
    }

    @Override
    public Map<String, Object> propertiesAsMap(final String... fieldNames) {
      ++counter.maps;
      return super.propertiesAsMap(fieldNames);
    }
  }

  private static final class CountingVertex extends ImmutableVertex {
    private final Counter counter;

    CountingVertex(final Database database, final DocumentType type, final RID rid, final Counter counter) {
      super(database, type, rid, null);
      this.counter = counter;
    }

    @Override
    public Object get(final String propertyName) {
      ++counter.gets;
      return super.get(propertyName);
    }

    @Override
    public Object getIfPresent(final String propertyName, final Object absentValue) {
      ++counter.gets;
      return super.getIfPresent(propertyName, absentValue);
    }

    @Override
    public Set<String> getPropertyNames() {
      ++counter.names;
      return super.getPropertyNames();
    }

    @Override
    public Map<String, Object> toMap(final boolean includeMetadata) {
      ++counter.maps;
      return super.toMap(includeMetadata);
    }

    @Override
    public Map<String, Object> propertiesAsMap() {
      ++counter.maps;
      return super.propertiesAsMap();
    }

    @Override
    public Map<String, Object> propertiesAsMap(final String... fieldNames) {
      ++counter.maps;
      return super.propertiesAsMap(fieldNames);
    }
  }

  private static final class CountingEdge extends ImmutableEdge {
    private final Counter counter;

    CountingEdge(final Database database, final DocumentType type, final RID rid, final Counter counter) {
      super(database, type, rid, null);
      this.counter = counter;
    }

    @Override
    public Object get(final String propertyName) {
      ++counter.gets;
      return super.get(propertyName);
    }

    @Override
    public Object getIfPresent(final String propertyName, final Object absentValue) {
      ++counter.gets;
      return super.getIfPresent(propertyName, absentValue);
    }

    @Override
    public Set<String> getPropertyNames() {
      ++counter.names;
      return super.getPropertyNames();
    }

    @Override
    public Map<String, Object> toMap(final boolean includeMetadata) {
      ++counter.maps;
      return super.toMap(includeMetadata);
    }

    @Override
    public Map<String, Object> propertiesAsMap() {
      ++counter.maps;
      return super.propertiesAsMap();
    }

    @Override
    public Map<String, Object> propertiesAsMap(final String... fieldNames) {
      ++counter.maps;
      return super.propertiesAsMap(fieldNames);
    }
  }
}
