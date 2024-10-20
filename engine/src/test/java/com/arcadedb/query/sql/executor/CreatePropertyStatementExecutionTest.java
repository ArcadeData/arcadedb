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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreatePropertyStatementExecutionTest extends TestHelper {
  private static final String PROP_NAME     = "name";
  private static final String PROP_DIVISION = "division";
  private static final String PROP_OFFICERS = "officers";
  private static final String PROP_ID       = "id";

  @Test
  public void testBasicCreateProperty() {
    database.command("sql", "create document type testBasicCreateProperty").close();
    database.command("sql", "CREATE property testBasicCreateProperty.name STRING").close();

    final DocumentType companyClass = database.getSchema().getType("testBasicCreateProperty");
    final Property nameProperty = companyClass.getProperty(PROP_NAME);

    assertThat(nameProperty.getName()).isEqualTo(PROP_NAME);
    assertThat(nameProperty.getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() {
    database.command("sql", "create document type testCreateMandatoryPropertyWithEmbeddedType").close();
    database.command("sql", "CREATE Property testCreateMandatoryPropertyWithEmbeddedType.officers LIST").close();

    final DocumentType companyClass = database.getSchema().getType("testCreateMandatoryPropertyWithEmbeddedType");
    final Property nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertThat(nameProperty.getName()).isEqualTo(PROP_OFFICERS);
    assertThat(nameProperty.getType()).isEqualTo(Type.LIST);
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() {
    database.command("sql", "create document type testCreateUnsafePropertyWithEmbeddedType").close();
    database.command("sql", "CREATE Property testCreateUnsafePropertyWithEmbeddedType.officers LIST").close();

    final DocumentType companyClass = database.getSchema().getType("testCreateUnsafePropertyWithEmbeddedType");
    final Property nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertThat(nameProperty.getName()).isEqualTo(PROP_OFFICERS);
    assertThat(nameProperty.getType()).isEqualTo(Type.LIST);
  }

  @Test
  public void testExtraSpaces() {
    database.command("sql", "create document type testExtraSpaces").close();
    database.command("sql", "CREATE PROPERTY testExtraSpaces.id INTEGER  ").close();

    final DocumentType companyClass = database.getSchema().getType("testExtraSpaces");
    final Property idProperty = companyClass.getProperty(PROP_ID);

    assertThat(idProperty.getName()).isEqualTo(PROP_ID);
    assertThat(idProperty.getType()).isEqualTo(Type.INTEGER);
  }

  @Test
  public void testInvalidAttributeName() {
    try {
      database.command("sql", "create document type CommandExecutionException").close();
      database.command("sql", "CREATE PROPERTY CommandExecutionException.id INTEGER (MANDATORY, INVALID, NOTNULL)  UNSAFE").close();
      fail("Expected CommandSQLParsingException");
    } catch (final CommandSQLParsingException e) {
      // OK
    }
  }

  @Test
  public void testLinkedTypeConstraint() {
    database.command("sql", "create document type Invoice").close();
    database.command("sql", "create document type Product").close();
    database.command("sql", "CREATE PROPERTY Invoice.products LIST of Product").close();
    database.command("sql", "CREATE PROPERTY Invoice.tags LIST of String").close();
    database.command("sql", "CREATE PROPERTY Invoice.settings MAP of String").close();
    database.command("sql", "CREATE PROPERTY Invoice.mainProduct LINK of Product").close();
    database.command("sql", "CREATE PROPERTY Invoice.embedded EMBEDDED of Product").close();

    final DocumentType mandatoryClass = database.getSchema().getType("Product");

    final DocumentType invoiceType = database.getSchema().getType("Invoice");
    final Property productsProperty = invoiceType.getProperty("products");
    assertThat(productsProperty.getName()).isEqualTo("products");
    assertThat(productsProperty.getType()).isEqualTo(Type.LIST);
    assertThat(productsProperty.getOfType()).isEqualTo("Product");

    final Property tagsProperty = invoiceType.getProperty("tags");
    assertThat(tagsProperty.getName()).isEqualTo("tags");
    assertThat(tagsProperty.getType()).isEqualTo(Type.LIST);
    assertThat(tagsProperty.getOfType()).isEqualTo("STRING");

    final Property settingsProperty = invoiceType.getProperty("settings");
    assertThat(settingsProperty.getName()).isEqualTo("settings");
    assertThat(settingsProperty.getType()).isEqualTo(Type.MAP);
    assertThat(settingsProperty.getOfType()).isEqualTo("STRING");

    final Property mainProductProperty = invoiceType.getProperty("mainProduct");
    assertThat(mainProductProperty.getName()).isEqualTo("mainProduct");
    assertThat(mainProductProperty.getType()).isEqualTo(Type.LINK);
    assertThat(mainProductProperty.getOfType()).isEqualTo("Product");

    final Property embeddedProperty = invoiceType.getProperty("embedded");
    assertThat(embeddedProperty.getName()).isEqualTo("embedded");
    assertThat(embeddedProperty.getType()).isEqualTo(Type.EMBEDDED);
    assertThat(embeddedProperty.getOfType()).isEqualTo("Product");

    final MutableDocument[] validInvoice = new MutableDocument[1];
    database.transaction(() -> {
      final MutableDocument linked = database.newDocument("Product").save();

      validInvoice[0] = database.newDocument("Invoice").set("products", List.of(linked));
      validInvoice[0].set("tags", List.of("tons of money", "hard to close"));
      validInvoice[0].set("settings", Map.of("locale", "US"));
      validInvoice[0].set("mainProduct", linked);
      validInvoice[0].newEmbeddedDocument("Product", "embedded");
      validInvoice[0].save();
    });

    try {
      database.transaction(() -> {
        database.newDocument("Invoice").set("products",//
            List.of(database.newDocument("Invoice").save())).save();
      });
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }

    try {
      validInvoice[0].set("tags", List.of(3, "hard to close")).save();
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }

    try {
      validInvoice[0].set("settings", Map.of("test", 10F)).save();
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }

    try {
      database.transaction(() -> {
        validInvoice[0].set("mainProduct", database.newDocument("Invoice").save()).save();
      });
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }

    try {
      database.transaction(() -> {
        validInvoice[0].newEmbeddedDocument("Invoice", "embedded").save();
      });
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }
  }

  @Test
  public void testIfNotExists() {
    database.command("sql", "create document type testIfNotExists").close();
    database.command("sql", "CREATE property testIfNotExists.name if not exists STRING").close();

    DocumentType clazz = database.getSchema().getType("testIfNotExists");
    Property nameProperty = clazz.getProperty(PROP_NAME);

    assertThat(nameProperty.getName()).isEqualTo(PROP_NAME);
    assertThat(nameProperty.getType()).isEqualTo(Type.STRING);

    database.command("sql", "CREATE property testIfNotExists.name if not exists STRING").close();

    clazz = database.getSchema().getType("testIfNotExists");
    nameProperty = clazz.getProperty(PROP_NAME);

    assertThat(nameProperty.getName()).isEqualTo(PROP_NAME);
    assertThat(nameProperty.getType()).isEqualTo(Type.STRING);
  }
}
