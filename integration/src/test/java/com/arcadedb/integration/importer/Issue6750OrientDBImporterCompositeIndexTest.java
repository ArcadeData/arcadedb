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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6750. The OrientDB importer only understood the single-property index shape, so an
 * `OCompositeIndexDefinition` - a multi-column UNIQUE constraint the source database enforced - was dropped with
 * nothing but a verbose-level warning, and the migrated database happily accepted the duplicates OrientDB rejected.
 * The optional `nullValuesIgnored` flag on the same path was unboxed unguarded and could NPE the whole index phase.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6750OrientDBImporterCompositeIndexTest {

  private static final String DATABASE_PATH = "target/databases/issue-6750-orientdb-composite-index";

  private static final String PERSON_CLASS = """
      {"name":"Person","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin",\
      "properties":[{"name":"firstName","type":"STRING","collate":"default"},\
      {"name":"lastName","type":"STRING","collate":"default"}]}""";

  /**
   * A composite definition as OrientDB's `OCompositeIndexDefinition.toStream()` writes it: the fields live in the
   * nested `indexDefinitions` array, there is no top-level `field`/`keyType`, and - the second half of the issue -
   * `nullValuesIgnored` is absent.
   */
  private static final String COMPOSITE_INDEX = """
      {"@type":"d","@version":0,"valueContainerAlgorithm":"NONE","name":"Person.name_surname","indexVersion":4,\
      "indexDefinition":{"@type":"d","@version":0,"className":"Person","indexDefinitions":[\
      {"@type":"d","@version":0,"collate":"default","field":"firstName","nullValuesIgnored":true,\
      "className":"Person","keyType":"STRING"},\
      {"@type":"d","@version":0,"collate":"default","field":"lastName","nullValuesIgnored":true,\
      "className":"Person","keyType":"STRING"}],\
      "indClasses":["com.orientechnologies.orient.core.index.OPropertyIndexDefinition",\
      "com.orientechnologies.orient.core.index.OPropertyIndexDefinition"]},\
      "type":"UNIQUE",\
      "indexDefinitionClass":"com.orientechnologies.orient.core.index.OCompositeIndexDefinition",\
      "clusters":["person"],"algorithm":"CELL_BTREE","@fieldTypes":"clusters=e"}""";

  @TempDir
  Path tempDir;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void aCompositeUniqueIndexSurvivesTheMigration() throws Exception {
    final File export = tempDir.resolve("composite-export.gz").toFile();

    OrientDBExportFixture.write(export, "composite", PERSON_CLASS, COMPOSITE_INDEX,
        """
        {"@type":"d","@rid":"#3:0","@class":"Person","@version":1,"firstName":"Jay","lastName":"Miner"}""");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(true);
        assertThat(indexes).hasSize(1);

        final TypeIndex index = indexes.iterator().next();
        assertThat(index.getPropertyNames()).containsExactly("firstName", "lastName");
        assertThat(index.isUnique()).isTrue();

        // THE CONSTRAINT THE SOURCE DATABASE ENFORCED MUST STILL BE ENFORCED HERE.
        assertThatThrownBy(() -> database.transaction(
            () -> database.newDocument("Person").set("firstName", "Jay").set("lastName", "Miner").save()))
            .isInstanceOf(DuplicatedKeyException.class);

        // A DIFFERENT PAIR SHARING THE FIRST COMPONENT IS STILL ACCEPTED: THE KEY IS THE PAIR, NOT THE FIRST FIELD.
        database.transaction(
            () -> database.newDocument("Person").set("firstName", "Jay").set("lastName", "Garulli").save());
        assertThat(database.countType("Person", true)).isEqualTo(2);
      } finally {
        database.drop();
      }
    }
  }

  /**
   * The nested definitions also carry the key types, so a composite index over properties the schema never declared
   * can still be created: each missing property is declared from its own nested `keyType`.
   */
  @Test
  void undeclaredPropertiesAreCreatedFromTheNestedKeyTypes() throws Exception {
    final File export = tempDir.resolve("undeclared-export.gz").toFile();

    OrientDBExportFixture.write(export, "undeclared",
        """
        {"name":"Person","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin"}""",
        """
        {"@type":"d","@version":0,"name":"Person.zone_code","indexVersion":4,\
        "indexDefinition":{"@type":"d","@version":0,"className":"Person","indexDefinitions":[\
        {"@type":"d","field":"zone","className":"Person","keyType":"STRING"},\
        {"@type":"d","field":"code","className":"Person","keyType":"INTEGER"}]},\
        "type":"NOTUNIQUE",\
        "indexDefinitionClass":"com.orientechnologies.orient.core.index.OCompositeIndexDefinition"}""",
        "");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        assertThat(database.getSchema().getType("Person").getProperty("zone").getType()).isEqualTo(Type.STRING);
        assertThat(database.getSchema().getType("Person").getProperty("code").getType()).isEqualTo(Type.INTEGER);

        final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(true);
        assertThat(indexes).hasSize(1);
        assertThat(indexes.iterator().next().getPropertyNames()).containsExactly("zone", "code");
        assertThat(indexes.iterator().next().isUnique()).isFalse();
      } finally {
        database.drop();
      }
    }
  }

  /**
   * An index whose key types are only partly mappable is skipped whole. A composite whose first field is mappable and
   * whose second is not must not leave the first field's property behind on the type: the property would then exist
   * for an index that was never built, and the only trace would be a verbose-level warning.
   */
  @Test
  void anUnmappableFieldLeavesNoHalfDeclaredSchemaBehind() throws Exception {
    final File export = tempDir.resolve("unmappable-export.gz").toFile();

    OrientDBExportFixture.write(export, "unmappable",
        """
        {"name":"Person","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin"}""",
        """
        {"@type":"d","@version":0,"name":"Person.zone_tags","indexVersion":4,\
        "indexDefinition":{"@type":"d","@version":0,"className":"Person","indexDefinitions":[\
        {"@type":"d","field":"zone","className":"Person","keyType":"STRING"},\
        {"@type":"d","field":"tags","className":"Person","keyType":"LINKBAG"}]},\
        "type":"NOTUNIQUE",\
        "indexDefinitionClass":"com.orientechnologies.orient.core.index.OCompositeIndexDefinition"}""",
        "");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        assertThat(database.getSchema().getType("Person").getAllIndexes(true)).isEmpty();

        // `zone` IS MAPPABLE AND COMES FIRST, BUT ITS INDEX WAS NEVER BUILT, SO IT MUST NOT HAVE BEEN DECLARED.
        assertThat(database.getSchema().getType("Person").existsProperty("zone")).isFalse();
        assertThat(database.getSchema().getType("Person").existsProperty("tags")).isFalse();
      } finally {
        database.drop();
      }
    }
  }

  /**
   * A single-property definition that omits the optional `nullValuesIgnored` flag used to unbox a null and abort the
   * creation of every remaining index of the import.
   */
  @Test
  void aMissingNullValuesIgnoredFlagDoesNotAbortTheIndexPhase() throws Exception {
    final File export = tempDir.resolve("no-flag-export.gz").toFile();

    OrientDBExportFixture.write(export, "noflag", PERSON_CLASS,
        """
        {"@type":"d","@version":0,"name":"Person.firstName","indexVersion":4,\
        "indexDefinition":{"@type":"d","@version":0,"collate":"default","field":"firstName",\
        "className":"Person","keyType":"STRING"},"type":"UNIQUE",\
        "indexDefinitionClass":"com.orientechnologies.orient.core.index.OPropertyIndexDefinition"},\
        {"@type":"d","@version":0,"name":"Person.lastName","indexVersion":4,\
        "indexDefinition":{"@type":"d","@version":0,"collate":"default","field":"lastName",\
        "nullValuesIgnored":false,"className":"Person","keyType":"STRING"},"type":"NOTUNIQUE",\
        "indexDefinitionClass":"com.orientechnologies.orient.core.index.OPropertyIndexDefinition"}""",
        "");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        // BOTH INDEXES: THE ONE WITHOUT THE FLAG, AND THE ONE THAT USED TO BE LOST WITH IT WHEN THE NPE HIT.
        assertThat(database.getSchema().getType("Person").getAllIndexes(true)).hasSize(2);
      } finally {
        database.drop();
      }
    }
  }
}
