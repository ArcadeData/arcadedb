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
import com.arcadedb.serializer.json.JSONObject;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlterPropertyExecutionTest extends TestHelper {
  @Test
  public void sqlAlterPropertyCustom() {
    database.command("sql", "CREATE VERTEX TYPE Car");
    assertThat(database.getSchema().getType("Car").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "CREATE PROPERTY Car.name STRING");
    assertThat(database.getSchema().getType("Car").existsProperty("name")).isTrue();
    assertThat(database.getSchema().getType("Car").getProperty("name").getType()).isEqualTo(Type.STRING);

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM description = 'test'");
    assertThat(database.getSchema().getType("Car").getProperty("name").getCustomValue("description")).isEqualTo("test");

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM age = 3");
    assertThat(database.getSchema().getType("Car").getProperty("name").getCustomValue("age")).isEqualTo(3);

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    final JSONObject customMap = cfg.getJSONObject("types").getJSONObject("Car").getJSONObject("properties").getJSONObject("name")
        .getJSONObject("custom");
    assertThat(customMap.getString("description")).isEqualTo("test");
    assertThat(customMap.getInt("age")).isEqualTo(3);

    database.close();
    database = factory.open();

    assertThat(database.getSchema().getType("Car").getProperty("name").getCustomValue("description")).isEqualTo("test");
    assertThat(((Number) database.getSchema().getType("Car").getProperty("name").getCustomValue("age")).intValue()).isEqualTo(3);

    database.command("sql", "ALTER PROPERTY Car.name CUSTOM age = null");
    assertThat(database.getSchema().getType("Car").getProperty("name").getCustomValue("age")).isNull();
    assertThat(database.getSchema().getType("Car").getProperty("name").getCustomKeys().contains("age")).isFalse();

    final ResultSet resultset = database.query("sql", "SELECT properties FROM schema:types");
    while (resultset.hasNext()) {
      final Result result = resultset.next();
      List properties =  result.getProperty("properties");

      final Object custom = ((Result) properties.get(0)).getProperty("custom");
      assertThat(custom instanceof Map).isTrue();
      assertThat((Map<?, ?>) custom).isNotEmpty();
    }
  }

  @Test
  public void sqlAlterPropertyDefault() {
    database.command("sql", "CREATE VERTEX TYPE Vehicle");
    database.command("sql", "CREATE PROPERTY Vehicle.id STRING");
    database.command("sql", "ALTER PROPERTY Vehicle.id DEFAULT \"12345\"");
    assertThat(database.getSchema().getType("Vehicle").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "CREATE VERTEX TYPE Car EXTENDS Vehicle");
    assertThat(database.getSchema().getType("Car").getSuperTypes().isEmpty()).isFalse();

    database.command("sql", "CREATE PROPERTY Car.name STRING");
    assertThat(database.getSchema().getType("Car").existsProperty("name")).isTrue();
    assertThat(database.getSchema().getType("Car").getProperty("name").getType()).isEqualTo(Type.STRING);

    database.command("sql", "ALTER PROPERTY Car.name DEFAULT \"test\"");
    assertThat(database.getSchema().getType("Car").getProperty("name").getDefaultValue()).isEqualTo("test");

    database.command("sql", "CREATE VERTEX TYPE Suv EXTENDS Car");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().isEmpty()).isFalse();

    database.command("sql", "CREATE PROPERTY Suv.weight float");
    assertThat(database.getSchema().getType("Suv").existsProperty("weight")).isTrue();
    assertThat(database.getSchema().getType("Suv").getProperty("weight").getType()).isEqualTo(Type.FLOAT);

    database.command("sql", "ALTER PROPERTY Suv.weight DEFAULT 1");
    assertThat(database.getSchema().getType("Suv").getProperty("weight").getDefaultValue()).isEqualTo(1.0F);

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    final String def1 = cfg.getJSONObject("types").getJSONObject("Car").getJSONObject("properties").getJSONObject("name")
        .getString("default");
    assertThat(def1).isEqualTo("\"test\"");
    final Float def2 = cfg.getJSONObject("types").getJSONObject("Suv").getJSONObject("properties").getJSONObject("weight")
        .getFloat("default");
    assertThat(def2).isEqualTo(1);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Car");
      final ResultSet result = database.command("sql", "SELECT FROM Car");
      assertThat(result.hasNext()).isTrue();

      final Vertex v = result.next().getVertex().get();
      assertThat(v.get("name")).isEqualTo("test");
    });

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Suv");
      final ResultSet result = database.command("sql", "SELECT FROM Suv");
      assertThat(result.hasNext()).isTrue();

      final Vertex v = result.next().getVertex().get();
      assertThat(v.get("id")).isEqualTo("12345");
      assertThat(v.get("name")).isEqualTo("test");
      assertThat(v.get("weight")).isEqualTo(1.0F);
    });

    database.close();
    database = factory.open();

    assertThat(database.getSchema().getType("Car").getProperty("name").getDefaultValue()).isEqualTo("test");

    database.command("sql", "ALTER PROPERTY Car.name DEFAULT null");
    assertThat(database.getSchema().getType("Car").getProperty("name").getDefaultValue()).isNull();
  }

  @Test
  public void sqlAlterPropertyDefaultFunctions() {
    database.command("sql", "CREATE VERTEX TYPE Log");
    assertThat(database.getSchema().getType("Log").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "CREATE PROPERTY Log.createdOn DATETIME_MICROS");
    assertThat(database.getSchema().getType("Log").existsProperty("createdOn")).isTrue();
    assertThat(database.getSchema().getType("Log").getProperty("createdOn").getType()).isEqualTo(Type.DATETIME_MICROS);

    database.command("sql", "ALTER PROPERTY Log.createdOn DEFAULT sysDate()");

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Log");

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      database.command("sql", "CREATE VERTEX Log");

      ResultSet result = database.command("sql", "SELECT FROM Log");
      assertThat(result.hasNext()).isTrue();

      Vertex v = result.next().getVertex().get();
      final LocalDateTime createdOn1 = v.getLocalDateTime("createdOn");
      assertThat(createdOn1).isNotNull();

      v = result.next().getVertex().get();
      final LocalDateTime createdOn2 = v.getLocalDateTime("createdOn");
      assertThat(createdOn2).isNotNull();

      assertThat(createdOn1).isNotEqualTo(createdOn2);

      v.modify().set("lastUpdateOn", LocalDateTime.now()).save();

      result = database.command("sql", "SELECT FROM Log");
      assertThat(result.hasNext()).isTrue();

      v = result.next().getVertex().get();
      assertThat(v.getLocalDateTime("createdOn")).isEqualTo(createdOn1);
    });
  }
}
