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
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
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

    final JSONObject cfg = database.getSchema().getEmbedded().serializeConfiguration();
    JSONObject customMap = cfg.getJSONObject("types").getJSONObject("Car").getJSONObject("properties").getJSONObject("name").getJSONObject("custom");
    Assertions.assertEquals("test", customMap.getString("description"));
    Assertions.assertEquals(3, customMap.getInt("age"));

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM age = null");
    Assertions.assertNull(database.getSchema().getType("Car").getProperty("name").getCustomValue("age"));
    Assertions.assertFalse(database.getSchema().getType("Car").getProperty("name").getCustomKeys().contains("age"));
  }
}
