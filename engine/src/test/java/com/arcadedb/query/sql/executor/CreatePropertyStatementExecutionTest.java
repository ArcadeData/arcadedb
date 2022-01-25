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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreatePropertyStatementExecutionTest extends TestHelper {
  private static final String PROP_NAME     = "name";
  private static final String PROP_DIVISION = "division";
  private static final String PROP_OFFICERS = "officers";
  private static final String PROP_ID       = "id";

  @Test
  public void testBasicCreateProperty() throws Exception {
    database.command("sql", "create document type testBasicCreateProperty").close();
    database.command("sql", "CREATE property testBasicCreateProperty.name STRING").close();

    DocumentType companyClass = database.getSchema().getType("testBasicCreateProperty");
    Property nameProperty = companyClass.getProperty(PROP_NAME);

    Assertions.assertEquals(nameProperty.getName(), PROP_NAME);
    Assertions.assertEquals(nameProperty.getType(), Type.STRING);
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() throws Exception {
    database.command("sql", "create document type testCreateMandatoryPropertyWithEmbeddedType").close();
    database.command("sql", "CREATE Property testCreateMandatoryPropertyWithEmbeddedType.officers LIST").close();

    DocumentType companyClass = database.getSchema().getType("testCreateMandatoryPropertyWithEmbeddedType");
    Property nameProperty = companyClass.getProperty(PROP_OFFICERS);

    Assertions.assertEquals(nameProperty.getName(), PROP_OFFICERS);
    Assertions.assertEquals(nameProperty.getType(), Type.LIST);
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() throws Exception {
    database.command("sql", "create document type testCreateUnsafePropertyWithEmbeddedType").close();
    database.command("sql", "CREATE Property testCreateUnsafePropertyWithEmbeddedType.officers LIST UNSAFE").close();

    DocumentType companyClass = database.getSchema().getType("testCreateUnsafePropertyWithEmbeddedType");
    Property nameProperty = companyClass.getProperty(PROP_OFFICERS);

    Assertions.assertEquals(nameProperty.getName(), PROP_OFFICERS);
    Assertions.assertEquals(nameProperty.getType(), Type.LIST);
  }

  @Test
  public void testExtraSpaces() throws Exception {
    database.command("sql", "create document type testExtraSpaces").close();
    database.command("sql", "CREATE PROPERTY testExtraSpaces.id INTEGER  ").close();

    DocumentType companyClass = database.getSchema().getType("testExtraSpaces");
    Property idProperty = companyClass.getProperty(PROP_ID);

    Assertions.assertEquals(idProperty.getName(), PROP_ID);
    Assertions.assertEquals(idProperty.getType(), Type.INTEGER);
  }

  public void testInvalidAttributeName() throws Exception {
    try {
      database.command("sql", "create document type CommandExecutionException").close();
      database.command("sql", "CREATE PROPERTY CommandExecutionException.id INTEGER (MANDATORY, INVALID, NOTNULL)  UNSAFE").close();
      Assertions.fail("Expected CommandExecutionException");
    } catch (CommandExecutionException e) {
      // OK
    }
  }

  @Test
  public void testMandatoryAsLinkedName() throws Exception {
    database.command("sql", "create document type testMandatoryAsLinkedName").close();
    database.command("sql", "create document type testMandatoryAsLinkedName_2").close();
    database.command("sql", "CREATE PROPERTY testMandatoryAsLinkedName.id LIST UNSAFE").close();

    DocumentType companyClass = database.getSchema().getType("testMandatoryAsLinkedName");
    DocumentType mandatoryClass = database.getSchema().getType("testMandatoryAsLinkedName_2");
    Property idProperty = companyClass.getProperty(PROP_ID);

    Assertions.assertEquals(idProperty.getName(), PROP_ID);
    Assertions.assertEquals(idProperty.getType(), Type.LIST);
  }

  @Test
  public void testIfNotExists() throws Exception {
    database.command("sql", "create document type testIfNotExists").close();
    database.command("sql", "CREATE property testIfNotExists.name if not exists STRING").close();

    DocumentType clazz = database.getSchema().getType("testIfNotExists");
    Property nameProperty = clazz.getProperty(PROP_NAME);

    Assertions.assertEquals(nameProperty.getName(), PROP_NAME);
    Assertions.assertEquals(nameProperty.getType(), Type.STRING);

    database.command("sql", "CREATE property testIfNotExists.name if not exists STRING").close();

    clazz = database.getSchema().getType("testIfNotExists");
    nameProperty = clazz.getProperty(PROP_NAME);

    Assertions.assertEquals(nameProperty.getName(), PROP_NAME);
    Assertions.assertEquals(nameProperty.getType(), Type.STRING);
  }
}
