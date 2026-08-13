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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;

import static com.arcadedb.integration.importer.OrientDBImporter.setImportedDefaultValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6134. OrientDB stores a property default as a plain value, ArcadeDB evaluates a String default as an SQL
 * expression, and since #6134 a bare token is rejected instead of silently evaluating to null. The importer bridges
 * the two: it imports the value as written when that is a valid expression, and quotes it otherwise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrientDBImporterDefaultValueTest {

  private static final String DATABASE_PATH = "target/databases/orientdb-import-default-values";

  private DatabaseFactory factory;
  private Database        database;
  private DocumentType    type;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    factory = new DatabaseFactory(DATABASE_PATH);
    database = factory.create();
    type = database.getSchema().createDocumentType("Imported");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    if (factory != null)
      factory.close();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void aBareTokenIsImportedAsAStringLiteral() {
    assertThat(setImportedDefaultValue(type.createProperty("status", Type.STRING), "active")).isTrue();

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Imported").save();
      assertThat(doc.getString("status")).isEqualTo("active");
    });
  }

  /**
   * The fallback keys off {@code SchemaException}, so it has to cover the rejection kind that originates as a parser
   * exception deep inside ANTLR, not only the bare-identifier one that is rejected without anything being thrown.
   * {@code compileDefaultValue} funnels both into the same single exit; this pins that.
   */
  @Test
  void aValueThatCannotBeParsedAtAllIsAlsoImportedAsAStringLiteral() {
    assertThat(setImportedDefaultValue(type.createProperty("note", Type.STRING), "this is (not parseable")).isTrue();

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Imported").save();
      assertThat(doc.getString("note")).isEqualTo("this is (not parseable");
    });
  }

  @Test
  void aQuoteInTheValueIsEscapedRatherThanClosingTheLiteral() {
    assertThat(setImportedDefaultValue(type.createProperty("quote", Type.STRING), "say \"hi\"")).isTrue();

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Imported").save();
      assertThat(doc.getString("quote")).isEqualTo("say \"hi\"");
    });
  }

  @Test
  void aFunctionIsImportedAsWrittenAndStillEvaluatedPerRecord() {
    assertThat(setImportedDefaultValue(type.createProperty("createdOn", Type.DATETIME_MICROS), "sysdate()")).isFalse();

    database.transaction(() -> {
      final Object createdOn = database.newDocument("Imported").save().get("createdOn");
      assertThat(createdOn).isInstanceOf(LocalDateTime.class);
    });
  }

  @Test
  void aNumberIsImportedAsWritten() {
    assertThat(setImportedDefaultValue(type.createProperty("weight", Type.INTEGER), "7")).isFalse();

    database.transaction(() -> assertThat(database.newDocument("Imported").save().getInteger("weight")).isEqualTo(7));
  }
}
