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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaTest extends TestHelper {

  @Test
  void tesSchemaSettings() {
    database.transaction(() -> {
      final ZoneId zoneId = database.getSchema().getZoneId();
      assertThat(zoneId).isNotNull();
      database.getSchema().setZoneId(ZoneId.of("America/New_York"));
      assertThat(database.getSchema().getZoneId()).isEqualTo(ZoneId.of("America/New_York"));

      final TimeZone timeZone = database.getSchema().getTimeZone();
      assertThat(timeZone).isNotNull();
      database.getSchema().setTimeZone(TimeZone.getTimeZone("UK"));
      assertThat(database.getSchema().getTimeZone()).isEqualTo(TimeZone.getTimeZone("UK"));

      final String dateFormat = database.getSchema().getDateFormat();
      assertThat(dateFormat).isNotNull();
      database.getSchema().setDateFormat("yyyy-MMM-dd");
      assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MMM-dd");

      final String dateTimeFormat = database.getSchema().getDateTimeFormat();
      assertThat(dateTimeFormat).isNotNull();
      database.getSchema().setDateTimeFormat("yyyy-MMM-dd HH:mm:ss");
      assertThat(database.getSchema().getDateTimeFormat()).isEqualTo("yyyy-MMM-dd HH:mm:ss");

      final String encoding = database.getSchema().getEncoding();
      assertThat(encoding).isNotNull();
      database.getSchema().setEncoding("UTF-8");
      assertThat(database.getSchema().getEncoding()).isEqualTo("UTF-8");
    });
  }

  @Test
  void renameDocument() {
    final DocumentType type = database.getSchema().createDocumentType("Doc1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Doc1").set("id", "id-" + i, "total", i).save();
    });

    type.rename("Doc2");

    assertThat(database.getSchema().existsType("Doc1")).isFalse();

    assertThat(database.getSchema().existsType("Doc2")).isTrue();
    assertThat(database.getSchema().getType("Doc2").getProperty("id").getType()).isEqualByComparingTo(Type.STRING);
    assertThat(database.getSchema().getType("Doc2").getProperty("total").getType()).isEqualByComparingTo(Type.DOUBLE);

    assertThat(database.countType("Doc2", true)).isEqualTo(100L);
  }

  @Test
  void renameVertexAndEdges() {
    final VertexType type = database.getSchema().createVertexType("V1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");

    final EdgeType edgeType = database.getSchema().createEdgeType("E1");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newVertex("V1").set("id", "id-" + i, "total", i).save();
    });

    type.rename("V2");
    edgeType.rename("E2");

    assertThat(database.getSchema().existsType("V1")).isFalse();

    assertThat(database.getSchema().existsType("V2")).isTrue();
    assertThat(database.getSchema().getType("V2").getProperty("id").getType()).isEqualByComparingTo(Type.STRING);
    assertThat(database.getSchema().getType("V2").getProperty("total").getType()).isEqualByComparingTo(Type.DOUBLE);

    assertThat(database.countType("V2", true)).isEqualTo(100L);

    assertThat(database.getSchema().existsType("E1")).isFalse();

    assertThat(database.getSchema().existsType("E2")).isTrue();

    for (ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
      assertThat(file.getFileName().contains("V1")).isFalse();
      assertThat(file.getFileName().contains("E1")).isFalse();
    }
  }

  @Test
  void aliases() {
    final VertexType type = database.getSchema().createVertexType("V1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");
    type.setAliases(Set.of("V11", "V111"));

    assertThat(database.getSchema().existsType("V1")).isTrue();
    assertThat(database.getSchema().existsType("V11")).isTrue();
    assertThat(database.getSchema().existsType("V111")).isTrue();

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newVertex("V1").set("id", "id-" + i, "total", i).save();
        database.newVertex("V11").set("id", "idd-" + i, "total", i).save();
        database.newVertex("V111").set("id", "iddd-" + i, "total", i).save();
      }
    });

    assertThat(database.getSchema().getType("V1").getName()).isEqualTo("V1");
    assertThat(database.getSchema().getType("V11").getName()).isEqualTo("V1");
    assertThat(database.getSchema().getType("V111").getName()).isEqualTo("V1");

    assertThat(database.countType("V1", true)).isEqualTo(300L);
    assertThat(database.countType("V11", true)).isEqualTo(300L);
    assertThat(database.countType("V111", true)).isEqualTo(300L);

    assertThat(database.select().fromType("V1").vertices().toList().size()).isEqualTo(300L);
    assertThat(database.select().fromType("V11").vertices().toList().size()).isEqualTo(300L);
    assertThat(database.select().fromType("V111").vertices().toList().size()).isEqualTo(300L);

    assertThat((Long) database.query("sql", "select count(*) as total from V1").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V11").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V111").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);

    database.close();
    reopenDatabase();

    assertThat((Long) database.query("sql", "select count(*) as total from V1").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V11").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V111").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
  }

  @Test
  void aliasesViaSQl() {
    final VertexType type = database.getSchema().createVertexType("V1");
    type.createProperty("id", Type.STRING);
    type.createProperty("total", Type.DOUBLE);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "total");

    database.command("sql", "alter type V1 aliases `V11`, `V111`");

    assertThat(database.getSchema().existsType("V1")).isTrue();
    assertThat(database.getSchema().existsType("V11")).isTrue();
    assertThat(database.getSchema().existsType("V111")).isTrue();

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newVertex("V1").set("id", "id-" + i, "total", i).save();
        database.newVertex("V11").set("id", "idd-" + i, "total", i).save();
        database.newVertex("V111").set("id", "iddd-" + i, "total", i).save();
      }
    });

    assertThat(database.getSchema().getType("V1").getName()).isEqualTo("V1");
    assertThat(database.getSchema().getType("V11").getName()).isEqualTo("V1");
    assertThat(database.getSchema().getType("V111").getName()).isEqualTo("V1");

    assertThat(database.countType("V1", true)).isEqualTo(300L);
    assertThat(database.countType("V11", true)).isEqualTo(300L);
    assertThat(database.countType("V111", true)).isEqualTo(300L);

    assertThat(database.select().fromType("V1").vertices().toList().size()).isEqualTo(300L);
    assertThat(database.select().fromType("V11").vertices().toList().size()).isEqualTo(300L);
    assertThat(database.select().fromType("V111").vertices().toList().size()).isEqualTo(300L);

    assertThat((Long) database.query("sql", "select count(*) as total from V1").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V11").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V111").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);

    reopenDatabase();

    assertThat((Long) database.query("sql", "select count(*) as total from V1").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V11").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);
    assertThat((Long) database.query("sql", "select count(*) as total from V111").nextIfAvailable().getProperty("total")).isEqualTo(
        300L);

    database.command("sql", "alter type V1 aliases null");

    assertThat(database.getSchema().existsType("V1")).isTrue();
    assertThat(database.getSchema().existsType("V11")).isFalse();
    assertThat(database.getSchema().existsType("V111")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().existsType("V1")).isTrue();
    assertThat(database.getSchema().existsType("V11")).isFalse();
    assertThat(database.getSchema().existsType("V111")).isFalse();
  }

  @Test
  void testDropNonExistentIndexThenCreateProperty() {
    // Reproduce the issue where DROP INDEX IF EXISTS for a non-existent index
    // prevents subsequent schema changes from being persisted
    database.command("sql", "CREATE DOCUMENT TYPE PSM_StructuralEntity IF NOT EXISTS");
    database.command("sql", "CREATE VERTEX TYPE PSM_PolicyUserCommentInfo IF NOT EXISTS EXTENDS PSM_StructuralEntity");

    // Add some initial properties
    database.command("sql", "CREATE PROPERTY PSM_PolicyUserCommentInfo.userIdentifier IF NOT EXISTS STRING (MIN 3, MAX 100)");
    database.command("sql", "CREATE PROPERTY PSM_PolicyUserCommentInfo.userEmail IF NOT EXISTS STRING (MANDATORY TRUE, NOTNULL TRUE, MIN 3, MAX 100)");

    // Verify the initial properties exist
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("userIdentifier")).isTrue();
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("userEmail")).isTrue();

    reopenDatabase();

    // Verify persistence after reopen
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("userIdentifier")).isTrue();
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("userEmail")).isTrue();

    // This is the problematic command - dropping a non-existent index
    database.command("sql", "DROP INDEX `IdentifiableEntity[uniqueId]` IF EXISTS");

    // Now create additional properties - these should be persisted
    database.command("sql", "CREATE PROPERTY PSM_PolicyUserCommentInfo.text IF NOT EXISTS STRING (MIN 3, MAX 3000)");
    database.command("sql", "CREATE PROPERTY PSM_PolicyUserCommentInfo.commenterType IF NOT EXISTS STRING (MANDATORY TRUE, NOTNULL TRUE, MIN 3, MAX 3000)");

    // Verify the new properties exist before closing
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("text")).isTrue();
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("commenterType")).isTrue();

    reopenDatabase();

    // Verify properties persist after database reopen
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("text"))
        .as("Property 'text' should persist after database reopen")
        .isTrue();
    assertThat(database.getSchema().getType("PSM_PolicyUserCommentInfo").existsProperty("commenterType"))
        .as("Property 'commenterType' should persist after database reopen")
        .isTrue();
  }

  @Test
  void testDropTypeWithIndexesAndBuckets() {
    // Test that dropType() properly manages multipleUpdate flag when dropping indexes and buckets
    database.command("sql", "CREATE VERTEX TYPE TestType");
    database.command("sql", "CREATE PROPERTY TestType.prop1 STRING");
    database.command("sql", "CREATE PROPERTY TestType.prop2 INTEGER");
    database.command("sql", "CREATE INDEX ON TestType (prop1) UNIQUE");
    database.command("sql", "CREATE INDEX ON TestType (prop2) NOTUNIQUE");

    assertThat(database.getSchema().existsType("TestType")).isTrue();
    assertThat(database.getSchema().existsIndex("TestType[prop1]")).isTrue();
    assertThat(database.getSchema().existsIndex("TestType[prop2]")).isTrue();

    // Drop the type (which internally calls dropIndex multiple times)
    database.command("sql", "DROP TYPE TestType");

    // Create a new type to verify schema saves work after dropType
    database.command("sql", "CREATE DOCUMENT TYPE NewTypeAfterDrop");
    database.command("sql", "CREATE PROPERTY NewTypeAfterDrop.field STRING");

    reopenDatabase();

    // Verify the type was dropped and new type persisted
    assertThat(database.getSchema().existsType("TestType")).isFalse();
    assertThat(database.getSchema().existsType("NewTypeAfterDrop")).isTrue();
    assertThat(database.getSchema().getType("NewTypeAfterDrop").existsProperty("field")).isTrue();
  }
}
