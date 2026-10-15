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
import org.junit.jupiter.api.Test;

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
