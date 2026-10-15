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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10098: {@link Result#toPropertyMap(boolean)} of a row that carries a record and no projection content read the record one
 * property at a time, and {@code ImmutableDocument.get(name)} walks the record header from its first byte until it meets the name, so
 * N properties cost N+1 passes where the {@code toMap(false)} it replaced cost one. The ratio grew with the property count (1.7x at 10
 * properties, 3.6x at 160). The guarantee is the number of passes, not a faster line: reading the whole row costs one pass over the
 * record, whatever its shape, whatever the row drops.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10098ToPropertyMapSinglePassTest extends TestHelper {
  private static final int PROPERTIES = 40;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc10098");
    database.getSchema().createVertexType("Vtx10098");
  }

  @Test
  void readingTheWholeRowIsOnePassOverTheRecordWhateverItsPropertyCount() {
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);

    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(false);

    assertThat(map).hasSize(PROPERTIES + 3);
    assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
    assertThat(document.headerWalks()).as("passes over the record header").isEqualTo(1);
  }

  @Test
  void theMetadataSeedDoesNotCostAPass() {
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);

    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(true);

    assertThat(map).containsEntry("@type", "Doc10098").containsEntry("@cat", "d").containsEntry("@rid", rid.toString());
    assertThat(document.headerWalks()).isEqualTo(1);
  }

  @Test
  void aPropertyTheRowDroppedIsLeftOutAndStillCostsOnePass() {
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);
    final ResultInternal row = new ResultInternal(document);
    row.removeProperty("p3");

    final Map<String, Object> map = row.toPropertyMap(false);

    assertThat(map).doesNotContainKey("p3").containsKey("p4").hasSize(PROPERTIES + 2);
    assertThat(document.headerWalks()).isEqualTo(1);
  }

  /** The single pass answers exactly what the record's getters do, shape by shape. */
  @Test
  void theMapHoldsWhatTheRowGettersAnswer() {
    final RID[] link = new RID[1];
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      link[0] = database.newVertex("Vtx10098").set("name", "target").save().getIdentity();
      final MutableDocument doc = database.newDocument("Doc10098");
      doc.set("text", "hello").set("number", 42).set("flag", true).set("nothing", null);
      doc.set("list", List.of(1, 2, 3)).set("map", Map.of("k", "v"));
      doc.set("link", link[0]);
      rid[0] = doc.save().getIdentity();
    });

    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid[0]);
    final ResultInternal row = new ResultInternal(document);
    final Map<String, Object> map = row.toPropertyMap(false);

    assertThat(map.keySet()).containsExactlyInAnyOrderElementsOf(row.getPropertyNames());
    for (final String name : row.getPropertyNames())
      assertThat(map.get(name)).as(name).isEqualTo(row.getProperty(name));
    assertThat(map.get("link")).isEqualTo(link[0]);
    assertThat(map).containsKey("nothing");
  }

  /**
   * The single pass decodes the same entries and value types as a read by name, on a schema with typed properties, a default value, an
   * embedded document, temporal and decimal types, a list and a map.
   */
  @Test
  void theMapHoldsTheSameEntriesAndTypesAsAReadByNameOnATypedSchema() {
    database.getSchema().createDocumentType("Address10098");
    final DocumentType mixed = database.getSchema().createDocumentType("Mixed10098");
    mixed.createProperty("when", Type.DATETIME);
    mixed.createProperty("day", Type.DATE);
    mixed.createProperty("price", Type.DECIMAL);
    mixed.createProperty("tags", Type.LIST);
    mixed.createProperty("attributes", Type.MAP);
    mixed.createProperty("home", Type.EMBEDDED).setOfType("Address10098");
    mixed.createProperty("status", Type.STRING).setDefaultValue("'new'");

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Mixed10098");
      doc.set("when", LocalDateTime.of(2026, 10, 15, 12, 30, 45)).set("day", LocalDate.of(2026, 10, 15))
          .set("price", new BigDecimal("12.34")).set("tags", List.of("a", "b")).set("attributes", Map.of("k", 1));
      doc.newEmbeddedDocument("Address10098", "home").set("city", "Rome").set("zip", 118);
      rid[0] = doc.save().getIdentity();
    });

    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Mixed10098"), rid[0]);
    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(false);
    assertThat(document.headerWalks()).isEqualTo(1);

    final Document byName = database.lookupByRID(rid[0], true).asDocument();
    assertThat(map.keySet()).containsExactlyInAnyOrderElementsOf(byName.getPropertyNames());
    for (final String name : byName.getPropertyNames()) {
      final Object expected = byName.get(name);
      if (expected instanceof Document embedded)
        assertThat(((Document) map.get(name)).toMap(false)).as(name).isEqualTo(embedded.toMap(false));
      else
        assertThat(map.get(name)).as(name).isEqualTo(expected);
      assertThat(map.get(name) == null ? null : map.get(name).getClass()).as("type of " + name)
          .isEqualTo(expected == null ? null : expected.getClass());
    }
    assertThat(map.get("status")).isEqualTo("new");
  }

  /** Every shape of record a row can carry answers, by name, exactly what its property map holds: mutable, vertex, edge, detached. */
  @Test
  void everyRecordShapeAgreesWithItsGettersByName() {
    database.getSchema().createEdgeType("Edge10098");
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Vtx10098").set("name", "a").set("n", 1).set("nothing", null).save();
      final MutableVertex b = database.newVertex("Vtx10098").set("name", "b").save();
      rids[0] = a.getIdentity();
      rids[1] = a.newEdge("Edge10098", b, "weight", 2.5, "label", "x").getIdentity();
      rids[2] = createDocument(3);
    });

    final List<Document> shapes = List.of(database.lookupByRID(rids[0], true).asDocument(),
        database.lookupByRID(rids[1], true).asDocument(), database.lookupByRID(rids[2], true).asDocument(),
        database.lookupByRID(rids[2], true).asDocument().modify(), database.lookupByRID(rids[2], true).asDocument().detach());

    for (final Document shape : shapes) {
      final Map<String, Object> map = new ResultInternal(shape).toPropertyMap(false);
      assertThat(map.keySet()).as(shape.getClass().getSimpleName()).containsExactlyInAnyOrderElementsOf(shape.getPropertyNames());
      for (final String name : shape.getPropertyNames())
        assertThat(map.get(name)).as(shape.getClass().getSimpleName() + "." + name).isEqualTo(shape.get(name));
    }
  }

  /**
   * {@code ImmutableDocument.toMap()} adds its metadata to the map the serializer answers, without a copy: that holds for a record
   * with no properties too, whose map is the serializer's early-return one.
   */
  @Test
  void aRecordWithoutPropertiesAnswersAMapTheMetadataCanBeAddedTo() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc10098").save().getIdentity());

    final Document empty = database.lookupByRID(rid[0], true).asDocument();
    assertThat(empty.toMap(false)).isEmpty();
    assertThat(empty.toMap(true)).containsEntry("@type", "Doc10098").containsEntry("@cat", "d").containsEntry("@rid", rid[0].toString());
    assertThat(new ResultInternal(empty).toPropertyMap(false)).isEmpty();
    assertThat(new ResultInternal(empty).toPropertyMap(true)).containsEntry("@rid", rid[0].toString());
  }

  @Test
  void aVertexRowIsOnePassToo() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex vertex = database.newVertex("Vtx10098");
      for (int i = 0; i < PROPERTIES; i++)
        vertex.set("p" + i, "value-" + i);
      rid[0] = vertex.save().getIdentity();
    });

    final Document loaded = database.lookupByRID(rid[0], true).asDocument();
    assertThat(new ResultInternal(loaded).toPropertyMap(false)).hasSize(PROPERTIES);
  }

  /** A row off a real scan, the case of INSERT ... FROM SELECT, answers the same map as the record's own. */
  @Test
  void aRowOffAScanAnswersTheRecordsOwnMap() {
    createDocument(5);

    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc10098")) {
      final Result row = rs.next();
      assertThat(row.toPropertyMap(false)).isEqualTo(row.toElement().toMap(false));
    }
  }

  // --- helpers ---

  private RID createDocument(final int properties) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc10098");
      for (int i = 0; i < properties; i++)
        doc.set("p" + i, "value-" + i);
      doc.set("n", 7).set("flag", true).set("nothing", null);
      rid[0] = doc.save().getIdentity();
    });
    return rid[0];
  }
}
