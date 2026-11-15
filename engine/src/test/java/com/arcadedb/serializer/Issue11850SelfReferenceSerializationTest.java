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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11850: a record whose property holds the record itself (SQL {@code SET x = @this}, openCypher {@code SET n.z = n}),
 * in the statement that wrote it, still holds the Java object rather than its RID. Serializing it to JSON expanded the record
 * inside itself until the stack overflowed, so the statement answered HTTP 500 and rolled back. The inner occurrence is now
 * written as the record's RID, which is what a later read of the stored link returns.
 */
class Issue11850SelfReferenceSerializationTest extends TestHelper {

  @Test
  void aDocumentHoldingItselfSerializesTheInnerOccurrenceAsItsRid() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      final MutableDocument document = database.newDocument("D").set("name", "a").save();
      document.set("self", document);

      final JSONObject json = JsonSerializer.createJsonSerializer().serializeDocument(document);

      assertThat(json.getString("name")).isEqualTo("a");
      assertThat(json.getString("self")).isEqualTo(document.getIdentity().toString());
    });
  }

  @Test
  void aDocumentHoldingItselfInAListOrAMapSerializesTheInnerOccurrenceAsItsRid() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      final MutableDocument document = database.newDocument("D").save();
      document.set("list", List.of(1, document));
      document.set("map", Map.of("me", document));

      final JSONObject json = JsonSerializer.createJsonSerializer().serializeDocument(document);
      final String rid = document.getIdentity().toString();

      final JSONArray list = json.getJSONArray("list");
      assertThat(list.getInt(0)).isEqualTo(1);
      assertThat(list.getString(1)).isEqualTo(rid);
      assertThat(json.getJSONObject("map").getString("me")).isEqualTo(rid);
    });
  }

  @Test
  void twoDocumentsHoldingEachOtherExpandTheOtherOnceThenStopAtTheRid() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      final MutableDocument a = database.newDocument("D").set("name", "a").save();
      final MutableDocument b = database.newDocument("D").set("name", "b").save();
      a.set("other", b);
      b.set("other", a);

      final JSONObject json = JsonSerializer.createJsonSerializer().serializeDocument(a);

      final JSONObject other = json.getJSONObject("other");
      assertThat(other.getString("name")).isEqualTo("b");
      assertThat(other.getString("other")).isEqualTo(a.getIdentity().toString());
    });
  }

  @Test
  void aDocumentReferencedTwiceSideBySideIsExpandedBothTimes() {
    // only an ancestor of the value being written is cut short: a sibling occurrence is not a cycle
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      final MutableDocument shared = database.newDocument("D").set("name", "shared").save();
      final MutableDocument owner = database.newDocument("D").save();
      owner.set("pair", List.of(shared, shared));

      final JSONArray pair = JsonSerializer.createJsonSerializer().serializeDocument(owner).getJSONArray("pair");

      assertThat(pair.getJSONObject(0).getString("name")).isEqualTo("shared");
      assertThat(pair.getJSONObject(1).getString("name")).isEqualTo("shared");
    });
  }

  @Test
  void sqlUpdateSettingThisReturnsTheRowWithItsRid() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> database.newDocument("D").set("name", "a").save());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE D SET self = @this RETURN AFTER")) {
        final Result row = rs.next();
        final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, row);
        assertThat(json.getString("self")).isEqualTo(json.getString("@rid"));
      }
    });
  }

  @Test
  void openCypherSetOfTheNodeItselfReturnsItsRid() {
    database.getSchema().createVertexType("P");
    database.transaction(() -> database.newVertex("P").set("i", 1).save());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (n:P {i: 1}) SET n.z = n RETURN n.z AS z, n AS node")) {
        final Result row = rs.next();
        final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, row);
        final String rid = json.getJSONObject("node").getString("@rid");
        assertThat(json.getJSONObject("z").getString("@rid")).isEqualTo(rid);
        assertThat(json.getJSONObject("z").getString("z")).isEqualTo(rid);
        assertThat(json.getJSONObject("node").getString("z")).isEqualTo(rid);
      }
    });
  }

  @Test
  void openCypherSetOfAPathThroughTheNodeReturnsTheNodeAsItsRidInsideThePath() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> database.command("opencypher", "CREATE (:P {i: 1})-[:K]->(:P {i: 2})"));

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH p = (n:P {i: 1})-[:K]->() SET n.z = p RETURN n AS node, 1 AS one")) {
        final Result row = rs.next();
        final JSONObject node = JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("node");
        final JSONArray nodes = node.getJSONObject("z").getJSONArray("nodes");
        assertThat(nodes.getString(0)).isEqualTo(node.getString("@rid"));
        assertThat(nodes.getJSONObject(1).getInt("i")).isEqualTo(2);
      }
    });
  }

  @Test
  void aRecordOfAnotherDatabaseWithTheSameRidIsNotAnAncestor() {
    final String otherPath = getDatabasePath() + "-other";
    FileUtils.deleteRecursively(new File(otherPath));
    try (final DatabaseFactory otherFactory = new DatabaseFactory(otherPath)) {
      final Database other = otherFactory.create();
      try {
        database.getSchema().createDocumentType("D");
        other.getSchema().createDocumentType("D");
        final MutableDocument[] remote = new MutableDocument[1];
        other.transaction(() -> remote[0] = other.newDocument("D").set("name", "remote").save());

        database.transaction(() -> {
          final MutableDocument local = database.newDocument("D").set("name", "local").save();
          // the two records share their RID, each in its own database
          assertThat(local.getIdentity()).isEqualTo(remote[0].getIdentity());
          local.set("child", remote[0]);

          final JSONObject child = JsonSerializer.createJsonSerializer().serializeDocument(local).getJSONObject("child");
          assertThat(child.getString("name")).isEqualTo("remote");
        });
      } finally {
        other.drop();
      }
    }
  }
}
