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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5409 (discussion #1759): allow declaring CUSTOM metadata inline in CREATE ... TYPE and
 * CREATE PROPERTY, without a follow-up ALTER statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CreateTypeCustomMetadataTest extends TestHelper {

  @Test
  void createDocumentTypeWithSingleCustom() {
    database.command("sql", "CREATE DOCUMENT TYPE doc CUSTOM coolness = 10");

    final DocumentType type = database.getSchema().getType("doc");
    assertThat(type.getCustomValue("coolness")).isEqualTo(10);
    assertThat(type.getCustomKeys()).containsExactly("coolness");
  }

  @Test
  void createVertexTypeWithMultipleCustom() {
    database.command("sql", "CREATE VERTEX TYPE vex CUSTOM x = 'width', y = 'height'");

    final DocumentType type = database.getSchema().getType("vex");
    assertThat(type.getCustomValue("x")).isEqualTo("width");
    assertThat(type.getCustomValue("y")).isEqualTo("height");
  }

  @Test
  void createEdgeTypeWithCustom() {
    database.command("sql", "CREATE EDGE TYPE edg CUSTOM label = 'connects', weight = 3.5");

    final DocumentType type = database.getSchema().getType("edg");
    assertThat(type.getCustomValue("label")).isEqualTo("connects");
    assertThat(((Number) type.getCustomValue("weight")).doubleValue()).isEqualTo(3.5);
  }

  @Test
  void createTypeWithCustomCombinedWithOtherClauses() {
    database.command("sql", "CREATE DOCUMENT TYPE base");
    database.command("sql",
        "CREATE DOCUMENT TYPE derived IF NOT EXISTS EXTENDS base BUCKETS 2 PAGESIZE 262144 CUSTOM a = 1, b = 'two'");

    final DocumentType type = database.getSchema().getType("derived");
    assertThat(type.getSuperTypes().getFirst().getName()).isEqualTo("base");
    assertThat(type.getBuckets(false).size()).isEqualTo(2);
    assertThat(type.getCustomValue("a")).isEqualTo(1);
    assertThat(type.getCustomValue("b")).isEqualTo("two");
  }

  @Test
  void createEdgeTypeWithCustomAndUnidirectional() {
    database.command("sql", "CREATE EDGE TYPE uni UNIDIRECTIONAL CUSTOM k = 'v'");

    assertThat(database.getSchema().getType("uni").getCustomValue("k")).isEqualTo("v");
  }

  @Test
  void createPropertyWithCustomAfterAttributes() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.num INTEGER (readonly true) CUSTOM oflines = 1");

    final Property property = database.getSchema().getType("doc").getProperty("num");
    assertThat(property.isReadonly()).isTrue();
    assertThat(property.getCustomValue("oflines")).isEqualTo(1);
  }

  @Test
  void createPropertyWithCustomWithoutAttributes() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.name STRING CUSTOM unit = 'meters', scale = 2");

    final Property property = database.getSchema().getType("doc").getProperty("name");
    assertThat(property.getCustomValue("unit")).isEqualTo("meters");
    assertThat(property.getCustomValue("scale")).isEqualTo(2);
  }

  @Test
  void customValuesSurviveSchemaReload() {
    database.command("sql", "CREATE DOCUMENT TYPE doc CUSTOM coolness = 10");
    database.command("sql", "CREATE PROPERTY doc.num INTEGER CUSTOM oflines = 2");

    reopenDatabase();

    final DocumentType type = database.getSchema().getType("doc");
    assertThat(type.getCustomValue("coolness")).isEqualTo(10);
    assertThat(type.getProperty("num").getCustomValue("oflines")).isEqualTo(2);
  }

  @Test
  void createTypeCustomIsReportedInResult() {
    final ResultSet rs = database.command("sql", "CREATE DOCUMENT TYPE doc CUSTOM a = 1, b = 'x'");
    final Result result = rs.next();
    assertThat(result.<String>getProperty("typeName")).isEqualTo("doc");
    assertThat(result.<Object>getProperty("custom").toString()).contains("a=1").contains("b=x");
  }

  @Test
  void createTypeNamedCustomStillWorks() {
    // `CUSTOM` is an allowed identifier: make sure a type literally named "custom" is unaffected.
    database.command("sql", "CREATE DOCUMENT TYPE custom");
    assertThat(database.getSchema().existsType("custom")).isTrue();
    assertThat(database.getSchema().getType("custom").getCustomKeys()).isEmpty();
  }

  @Test
  void createPropertyNamedCustomStillWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.custom STRING");

    assertThat(database.getSchema().getType("doc").existsProperty("custom")).isTrue();
  }
}
