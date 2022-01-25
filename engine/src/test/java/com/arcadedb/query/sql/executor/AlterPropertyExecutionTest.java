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
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlterPropertyExecutionTest extends TestHelper {
  @Test
  public void sqlAlterPropertyCustom() {
    database.command("sql", "CREATE VERTEX TYPE Car");
    Assertions.assertTrue(database.getSchema().getType("Car").getSuperTypes().isEmpty());

    database.command("sql", "CREATE PROPERTY Car.name STRING");
    Assertions.assertTrue(database.getSchema().getType("Car").existsProperty("name"));
    Assertions.assertEquals(Type.STRING, database.getSchema().getType("Car").getProperty("name").getType());

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM description = 'test'");
    Assertions.assertEquals("test", database.getSchema().getType("Car").getProperty("name").getCustomValue("description"));

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM age = 3");
    Assertions.assertEquals(3, database.getSchema().getType("Car").getProperty("name").getCustomValue("age"));

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    JSONObject customMap = cfg.getJSONObject("types").getJSONObject("Car").getJSONObject("properties").getJSONObject("name").getJSONObject("custom");
    Assertions.assertEquals("test", customMap.getString("description"));
    Assertions.assertEquals(3, customMap.getInt("age"));

    database.close();
    database = factory.open();

    Assertions.assertEquals("test", database.getSchema().getType("Car").getProperty("name").getCustomValue("description"));
    Assertions.assertEquals(3, database.getSchema().getType("Car").getProperty("name").getCustomValue("age"));

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM age = null");
    Assertions.assertNull(database.getSchema().getType("Car").getProperty("name").getCustomValue("age"));
    Assertions.assertFalse(database.getSchema().getType("Car").getProperty("name").getCustomKeys().contains("age"));
  }

  @Test
  public void sqlAlterPropertyDefault() {
    database.command("sql", "CREATE VERTEX TYPE Car");
    Assertions.assertTrue(database.getSchema().getType("Car").getSuperTypes().isEmpty());

    database.command("sql", "CREATE PROPERTY Car.name STRING");
    Assertions.assertTrue(database.getSchema().getType("Car").existsProperty("name"));
    Assertions.assertEquals(Type.STRING, database.getSchema().getType("Car").getProperty("name").getType());

    database.command("sql", "ALTER PROPERTY Car.name DEFAULT 'test'");
    Assertions.assertEquals("test", database.getSchema().getType("Car").getProperty("name").getDefaultValue());

    database.command("sql", "CREATE VERTEX TYPE Suv EXTENDS Car");
    Assertions.assertFalse(database.getSchema().getType("Suv").getSuperTypes().isEmpty());

    database.command("sql", "CREATE PROPERTY Suv.weight float");
    Assertions.assertTrue(database.getSchema().getType("Suv").existsProperty("weight"));
    Assertions.assertEquals(Type.FLOAT, database.getSchema().getType("Suv").getProperty("weight").getType());

    database.command("sql", "ALTER PROPERTY Suv.weight DEFAULT 1");
    Assertions.assertEquals(1, database.getSchema().getType("Suv").getProperty("weight").getDefaultValue());

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    String def1 = cfg.getJSONObject("types").getJSONObject("Car").getJSONObject("properties").getJSONObject("name").getString("default");
    Assertions.assertEquals("test", def1);
    Float def2 = cfg.getJSONObject("types").getJSONObject("Suv").getJSONObject("properties").getJSONObject("weight").getFloat("default");
    Assertions.assertEquals(1, def2);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Car");
      ResultSet result = database.command("sql", "SELECT FROM Car");
      Assertions.assertTrue(result.hasNext());

      final Vertex v = result.next().getVertex().get();
      Assertions.assertEquals("test", v.get("name"));
    });

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Suv");
      ResultSet result = database.command("sql", "SELECT FROM Suv");
      Assertions.assertTrue(result.hasNext());

      final Vertex v = result.next().getVertex().get();
      Assertions.assertEquals("test", v.get("name"));
      Assertions.assertEquals(1.0F, v.get("weight"));
    });

    database.close();
    database = factory.open();

    Assertions.assertEquals("test", database.getSchema().getType("Car").getProperty("name").getDefaultValue());

    database.command("sql", "ALTER PROPERTY Car.name DEFAULT null");
    Assertions.assertNull(database.getSchema().getType("Car").getProperty("name").getDefaultValue());
  }
}
