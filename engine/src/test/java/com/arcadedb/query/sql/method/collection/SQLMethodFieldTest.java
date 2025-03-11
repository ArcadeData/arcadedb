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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodFieldTest extends TestHelper {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodField();

  }

  @Test
  void testNulIParamsReturnedAsNull() {
    final Object result = method.execute(null, null, null, new Object[] { null });
    assertThat(result).isNull();
  }

  @Test
  void testFieldValueSQL() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().buildDocumentType().withName("Test").create();
      type.createProperty("embedded", Type.EMBEDDED);
      MutableDocument document = database.newDocument("Test");
      MutableEmbeddedDocument embeddedDocument = document.newEmbeddedDocument("Test", "embedded");
      embeddedDocument.set("field", "value");
      document.set("embedded", embeddedDocument);
      document.save();

      ResultSet resultSet = database.query("sql", "SELECT embedded.field('field') as res FROM Test");
      Result result = resultSet.next();
      assertThat(result.<String>getProperty("res")).isEqualTo("value");
    });

  }
  @Test
  void testFieldValue() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().buildDocumentType().withName("Test").create();
      type.createProperty("embedded", Type.EMBEDDED);
      MutableDocument document = database.newDocument("Test");
      MutableEmbeddedDocument embeddedDocument = document.newEmbeddedDocument("Test", "embedded");
      embeddedDocument.set("field", "value");
      document.set("embedded", embeddedDocument);
      document.save();
      Object result = method.execute(embeddedDocument, document, null, new Object[] { "field" });

      assertThat(result).isInstanceOf(String.class);
      assertThat(result).isEqualTo("value");
    });

  }
}
