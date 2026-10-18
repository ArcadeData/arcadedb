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
import com.arcadedb.engine.Bucket;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10247: every schema change rebuilt the bucket-to-type maps over every type and assembled the whole schema
 * document before writing it, so a {@code CREATE TYPE} cost O(N) in the number of types and creating N types O(N^2). The
 * maps now follow the additive changes in place, and the schema file is written type by type.
 * <p>
 * The cost is pinned by counting the full rebuilds rather than by timing anything. The maps maintained in place are also
 * compared with a full rebuild at the end of every schema change whenever assertions are enabled, as they are in the
 * tests, so the whole suite checks them; the tests below check them explicitly across every kind of membership change.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10247SchemaChangeScalingTest extends TestHelper {

  @Test
  void creatingTypesAndPropertiesDoesNotRebuildTheBucketMaps() {
    final LocalSchema schema = localSchema();
    final long rebuildsBefore = schema.getBucketTypeMapRebuilds();

    for (int i = 0; i < 100; i++) {
      schema.createDocumentType("Doc" + i).createProperty("p", Type.STRING);
      schema.createVertexType("Vertex" + i).createProperty("p", Type.STRING);
      schema.createEdgeType("Edge" + i);
    }
    schema.getType("Doc0").createProperty("blob", Type.STRING).setExternal(true);
    database.command("sql", "CREATE BUCKET Extra");
    database.command("sql", "ALTER TYPE Doc1 BUCKET +Extra");

    assertThat(schema.getBucketTypeMapRebuilds()).isEqualTo(rebuildsBefore);
    assertBucketMapsMatchTheTypes(schema);
  }

  @Test
  void theBucketMapsFollowEveryKindOfMembershipChange() {
    final LocalSchema schema = localSchema();
    schema.createDocumentType("A");
    schema.createVertexType("V").createProperty("big", Type.STRING).setExternal(true);
    schema.createEdgeType("E");
    database.command("sql", "CREATE BUCKET Spare");
    database.command("sql", "ALTER TYPE A BUCKET +Spare");
    assertBucketMapsMatchTheTypes(schema);

    // a bucket belongs to one type only
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE V BUCKET +Spare")).hasMessageContaining("already associated");
    assertThatThrownBy(() -> database.command("sql", "DROP BUCKET Spare")).hasMessageContaining("used by type");

    database.command("sql", "ALTER TYPE A BUCKET -Spare");
    assertThat(schema.getTypeByBucketId(schema.getBucketByName("Spare").getFileId())).isNull();
    assertBucketMapsMatchTheTypes(schema);

    // detached, the bucket can be taken by another type, and dropped once free again
    database.command("sql", "ALTER TYPE V BUCKET +Spare");
    assertThat(schema.getTypeByBucketId(schema.getBucketByName("Spare").getFileId()).getName()).isEqualTo("V");
    database.command("sql", "ALTER TYPE V BUCKET -Spare");
    database.command("sql", "DROP BUCKET Spare");
    assertBucketMapsMatchTheTypes(schema);

    database.command("sql", "ALTER TYPE A NAME Renamed");
    assertBucketMapsMatchTheTypes(schema);

    schema.dropType("E");
    schema.dropType("V");
    assertBucketMapsMatchTheTypes(schema);

    schema.bulkChange(() -> {
      for (int i = 0; i < 20; i++)
        schema.createVertexType("Bulk" + i);
      schema.dropType("Bulk3");
    });
    assertBucketMapsMatchTheTypes(schema);

    reopenDatabase();
    assertBucketMapsMatchTheTypes(localSchema());
    assertThat(localSchema().existsType("Bulk3")).isFalse();
    assertThat(localSchema().existsType("Bulk19")).isTrue();
  }

  @Test
  void theSchemaFileIsTheDocumentTheSchemaDescribes() throws Exception {
    final LocalSchema schema = localSchema();
    final DocumentType doc = schema.createDocumentType("Doc \"quoted\" & <tagged>");
    doc.createProperty("name", Type.STRING).setMandatory(true).setDefaultValue("'x'");
    doc.createProperty("json", Type.MAP);
    doc.setCustomValue("note", "a\nmulti-line\tvalue è");
    schema.createVertexType("Person").createProperty("age", Type.INTEGER);
    database.command("sql", "CREATE INDEX ON Person (age) NOTUNIQUE");
    schema.createEdgeType("Knows");
    database.command("sql", "CREATE TRIGGER t1 BEFORE CREATE ON TYPE Person EXECUTE SQL 'SELECT 1'");

    assertThat(schema.serializeSchemaForTest()).isEqualTo(schema.toJSON().toString());

    // and what the last change wrote is that document
    final File file = schema.getConfigurationFile();
    try (final FileInputStream in = new FileInputStream(file)) {
      assertThat(FileUtils.readStreamAsString(in, "UTF-8")).isEqualTo(schema.toJSON().toString());
    }
  }

  @Test
  void aSchemaOfManyTypesReopensWithEveryType() {
    final LocalSchema schema = localSchema();
    schema.bulkChange(() -> {
      for (int i = 0; i < 300; i++)
        schema.createDocumentType("T" + i).createProperty("p", Type.STRING);
    });
    reopenDatabase();
    assertThat(localSchema().getTypes()).hasSize(300);
    assertBucketMapsMatchTheTypes(localSchema());
  }

  private LocalSchema localSchema() {
    return (LocalSchema) database.getSchema().getEmbedded();
  }

  /** Every bucket a type owns answers to it, and every bucket it involves answers to it unless another type owns it. */
  private static void assertBucketMapsMatchTheTypes(final LocalSchema schema) {
    int owned = 0;
    for (final DocumentType type : schema.getTypes()) {
      for (final Bucket bucket : type.getBuckets(false)) {
        assertThat(schema.getTypeByBucketId(bucket.getFileId())).as(bucket.getName()).isSameAs(type);
        assertThat(schema.getInvolvedTypeByBucketId(bucket.getFileId())).as(bucket.getName()).isSameAs(type);
        ++owned;
      }
      for (final Bucket bucket : type.getInvolvedBuckets()) {
        final DocumentType owner = schema.getTypeByBucketId(bucket.getFileId());
        assertThat(schema.getInvolvedTypeByBucketId(bucket.getFileId())).as(bucket.getName())
            .isSameAs(owner != null ? owner : type);
      }
    }
    // and no bucket answers to a type that does not own it
    int mapped = 0;
    for (final Bucket bucket : schema.getBuckets())
      if (schema.getTypeByBucketId(bucket.getFileId()) != null)
        ++mapped;
    assertThat(mapped).isEqualTo(owned);
  }
}
