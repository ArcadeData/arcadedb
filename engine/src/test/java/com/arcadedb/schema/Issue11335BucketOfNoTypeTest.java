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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11335: {@code ALTER TYPE ... BUCKET -x} leaves the bucket's records in place with no type, and every path that met one read
 * the type without a check: {@code UPDATE BUCKET:x SET ...} ended on a {@link NullPointerException}, and so did the other update
 * forms, a bare {@code save()}, {@code toJSON()} of a plain {@code SELECT}, and every delete, which left the records impossible to
 * clean up.
 * <p>
 * The contract: such a record can be read and deleted, and writing one is refused saying its bucket belongs to no type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11335BucketOfNoTypeTest extends TestHelper {
  private RID document;
  private RID vertex;

  @BeforeEach
  void detachBucketsHoldingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.id INTEGER");
    database.command("sql", "CREATE INDEX ON A (id) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE V1");
    database.command("sql", "CREATE BUCKET x");
    database.command("sql", "CREATE BUCKET y");
    database.command("sql", "ALTER TYPE A BUCKET +x");
    database.command("sql", "ALTER TYPE V1 BUCKET +y");

    database.transaction(() -> {
      document = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 1, \"on\": \"2026-11-06\" }");
      vertex = insert("INSERT INTO BUCKET:y CONTENT { \"id\": 2 }");
    });

    database.command("sql", "ALTER TYPE A BUCKET -x");
    database.command("sql", "ALTER TYPE V1 BUCKET -y");
  }

  private RID insert(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "UPDATE BUCKET:x SET v = 1", "UPDATE BUCKET:x CONTENT { \"v\": 1 }", "UPDATE BUCKET:x MERGE { \"v\": 1 }",
      "UPDATE BUCKET:x REMOVE id", "UPDATE BUCKET:y SET v = 1" })
  void everySqlUpdateIsRefusedSayingTheBucketBelongsToNoType(final String update) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", update).close()))//
        .hasMessageContaining("belongs to no type");
    assertThat(document.asDocument().get("v")).isNull();
  }

  @Test
  void updateOfTheRecordByRidIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + document + " SET v = 1").close()))//
        .hasMessageContaining("belongs to no type");
  }

  @Test
  void modifyIsRefusedForADocumentAndAVertex() {
    database.transaction(() -> {
      assertThatThrownBy(() -> document.asDocument().modify())//
          .isInstanceOf(SchemaException.class).hasMessageContaining("bucket 'x' belongs to no type");
      assertThatThrownBy(() -> vertex.asVertex().modify())//
          .isInstanceOf(SchemaException.class).hasMessageContaining("bucket 'y' belongs to no type");
    });
  }

  @Test
  void aRecordOfTheBucketReadsWithoutAType() {
    final Document read = document.asDocument();
    assertThat(read.getType()).isNull();
    assertThat(read.getTypeName()).isNull();
    assertThat(read.getInteger("id")).isEqualTo(1);
    assertThat(read.getLocalDate("on")).isEqualTo(LocalDate.of(2026, 11, 6));

    final JSONObject json = read.toJSON(true);
    assertThat(json.getInt("id")).isEqualTo(1);
    assertThat(json.has(Property.TYPE_PROPERTY)).isFalse();
    assertThat(read.toMap(true)).containsEntry("id", 1).doesNotContainKey(Property.TYPE_PROPERTY);
    assertThat(vertex.asVertex().toJSON(true).has(Property.TYPE_PROPERTY)).isFalse();
  }

  @Test
  void aSelectOfTheBucketSerializesItsRecords() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM BUCKET:x")) {
      final Result row = rs.next();
      assertThat(row.toJSON().getInt("id")).isEqualTo(1);
      assertThat(row.toMap()).containsEntry("id", 1);

      final JsonSerializer serializer = new JsonSerializer(database);
      assertThat(serializer.serializeResult(database, row).has(Property.TYPE_PROPERTY)).isFalse();
      assertThat(serializer.serializeDocument(row.toElement()).has(Property.TYPE_PROPERTY)).isFalse();
      assertThat(JsonGraphSerializer.createJsonGraphSerializer().serializeGraphElement(row.toElement()).has("t")).isFalse();
    }
  }

  @Test
  void aScanOfTheBucketReadsItsRecords() {
    final List<Record> scanned = new ArrayList<>();
    database.scanBucket("x", scanned::add);
    assertThat(scanned).hasSize(1);
    assertThat(((Document) scanned.getFirst()).getInteger("id")).isEqualTo(1);
  }

  @Test
  void deletingTheRecordsEmptiesTheBucket() {
    database.transaction(() -> database.command("sql", "DELETE FROM BUCKET:x").close());
    assertThat(database.countBucket("x")).isZero();

    database.transaction(() -> vertex.asVertex().delete());
    assertThat(database.countBucket("y")).isZero();
  }

  @Test
  void deletingARecordByRidAndThroughTheApiWorks() {
    final RID[] more = new RID[2];
    database.command("sql", "ALTER TYPE A BUCKET +x");
    database.transaction(() -> {
      more[0] = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 10 }");
      more[1] = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 11 }");
    });
    database.command("sql", "ALTER TYPE A BUCKET -x");

    database.transaction(() -> database.command("sql", "DELETE FROM " + more[0]).close());
    database.transaction(() -> more[1].asDocument().delete());
    assertThat(database.countBucket("x")).isEqualTo(1);
  }

  @Test
  void aVertexOfTheBucketDeletesWithItsEdges() {
    database.command("sql", "CREATE EDGE TYPE E1");
    database.command("sql", "CREATE VERTEX TYPE V2");
    database.command("sql", "ALTER TYPE V1 BUCKET +y");
    final RID[] other = new RID[1];
    database.transaction(() -> {
      final MutableVertex to = database.newVertex("V2").set("id", 3).save();
      other[0] = to.getIdentity();
      vertex.asVertex().modify().newEdge("E1", to);
    });
    database.command("sql", "ALTER TYPE V1 BUCKET -y");

    database.transaction(() -> vertex.asVertex().delete());

    assertThat(database.countBucket("y")).isZero();
    assertThat(database.countType("E1", true)).isZero();
    assertThat(other[0].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
  }

  @Test
  void truncatingTheBucketEmptiesIt() {
    database.command("sql", "TRUNCATE BUCKET x");
    assertThat(database.countBucket("x")).isZero();
  }

  @Test
  void theRecordIsWritableAgainOnceTheBucketIsAttached() {
    database.command("sql", "ALTER TYPE A BUCKET +x");

    database.transaction(() -> database.command("sql", "UPDATE BUCKET:x SET v = 1").close());

    final Document read = document.asDocument();
    assertThat(read.getTypeName()).isEqualTo("A");
    assertThat(read.getInteger("v")).isEqualTo(1);
  }

  @Test
  void aRecordSavedIntoABucketOfNoTypeReadsAndDeletesLikeTheOthers() {
    // THE OTHER WAY TO MAKE ONE: SAVING INTO A FREE BUCKET IS ALLOWED, AND READ BACK THE RECORD HAS NO TYPE
    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument created = database.newDocument("A").set("id", 99);
      created.save("x");
      saved[0] = created.getIdentity();
    });

    assertThat(database.query("sql", "SELECT FROM " + saved[0]).next().toJSON().getInt("id")).isEqualTo(99);

    database.transaction(() -> saved[0].asDocument().delete());
    assertThat(database.countBucket("x")).isEqualTo(1);
  }
}
