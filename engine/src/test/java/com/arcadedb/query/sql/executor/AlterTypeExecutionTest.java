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
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlterTypeExecutionTest extends TestHelper {
  @Test
  public void sqlAlterTypeInheritanceUsing() {
    database.command("sql", "CREATE VERTEX TYPE Car");

    Assertions.assertTrue(database.getSchema().getType("Car").getSuperTypes().isEmpty());

    database.command("sql", "CREATE VERTEX TYPE Vehicle");
    database.command("sql", "ALTER TYPE Car SUPERTYPE +Vehicle");

    Assertions.assertEquals(1, database.getSchema().getType("Car").getSuperTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Car").getSuperTypes().stream().map(x -> x.getName()).collect(Collectors.toSet()).contains("Vehicle"));
    Assertions.assertEquals(1, database.getSchema().getType("Vehicle").getSubTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Vehicle").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet()).contains("Car"));
    Assertions.assertTrue(database.getSchema().getType("Vehicle").isSuperTypeOf("Car"));

    database.command("sql", "CREATE VERTEX TYPE Suv");
    Assertions.assertTrue(database.getSchema().getType("Suv").getSuperTypes().isEmpty());

    database.command("sql", "ALTER TYPE Suv SUPERTYPE +Car");
    Assertions.assertEquals(1, database.getSchema().getType("Suv").getSuperTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Car").isSuperTypeOf("Suv"));
    Assertions.assertEquals(1, database.getSchema().getType("Car").getSubTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Car").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet()).contains("Suv"));
    Assertions.assertTrue(database.getSchema().getType("Car").isSuperTypeOf("Suv"));

    database.command("sql", "ALTER TYPE Car SUPERTYPE -Vehicle");
    Assertions.assertTrue(database.getSchema().getType("Car").getSuperTypes().isEmpty());
    Assertions.assertTrue(database.getSchema().getType("Vehicle").getSubTypes().isEmpty());

    database.command("sql", "ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
    Assertions.assertEquals(2, database.getSchema().getType("Suv").getSuperTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Car").isSuperTypeOf("Suv"));
    Assertions.assertTrue(database.getSchema().getType("Vehicle").isSuperTypeOf("Suv"));
    Assertions.assertEquals(1, database.getSchema().getType("Car").getSubTypes().size());
    Assertions.assertEquals(1, database.getSchema().getType("Vehicle").getSubTypes().size());
    Assertions.assertTrue(database.getSchema().getType("Car").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet()).contains("Suv"));
    Assertions.assertTrue(database.getSchema().getType("Car").isSuperTypeOf("Suv"));
    Assertions.assertTrue(database.getSchema().getType("Vehicle").isSuperTypeOf("Suv"));
  }

  @Test
  public void sqlAlterTypeCustom() {
    database.command("sql", "CREATE VERTEX TYPE Suv");

    database.command("sql", "ALTER TYPE Suv CUSTOM description = 'test'");
    Assertions.assertEquals("test", database.getSchema().getType("Suv").getCustomValue("description"));

    database.command("sql", "ALTER TYPE Suv CUSTOM age = 3");
    Assertions.assertEquals(3, database.getSchema().getType("Suv").getCustomValue("age"));

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    JSONObject customMap = cfg.getJSONObject("types").getJSONObject("Suv").getJSONObject("custom");
    Assertions.assertEquals("test", customMap.getString("description"));
    Assertions.assertEquals(3, customMap.getInt("age"));

    database.command("sql", "ALTER TYPE Suv CUSTOM age = null");
    Assertions.assertNull(database.getSchema().getType("Suv").getCustomValue("age"));
    Assertions.assertFalse(database.getSchema().getType("Suv").getCustomKeys().contains("age"));
  }
}
