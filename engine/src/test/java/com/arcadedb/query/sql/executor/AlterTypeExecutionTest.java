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
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlterTypeExecutionTest extends TestHelper {
  @Test
  public void sqlAlterTypeInheritanceUsing() {
    database.command("sql", "CREATE VERTEX TYPE Car");

    assertThat(database.getSchema().getType("Car").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "CREATE VERTEX TYPE Vehicle");
    database.command("sql", "ALTER TYPE Car SUPERTYPE +Vehicle");

    assertThat(database.getSchema().getType("Car").getSuperTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").getSuperTypes().stream().map(x -> x.getName()).collect(Collectors.toSet())
        .contains("Vehicle")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet())
        .contains("Car")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").isSuperTypeOf("Car")).isTrue();

    database.command("sql", "CREATE VERTEX TYPE Suv");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "ALTER TYPE Suv SUPERTYPE +Car");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet())
        .contains("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();

    database.command("sql", "ALTER TYPE Car SUPERTYPE -Vehicle");
    assertThat(database.getSchema().getType("Car").getSuperTypes().isEmpty()).isTrue();
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().isEmpty()).isTrue();

    database.command("sql", "ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().size()).isEqualTo(2);
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").getSubTypes().stream().map(x -> x.getName()).collect(Collectors.toSet())
        .contains("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").isSuperTypeOf("Suv")).isTrue();
  }

  @Test
  public void sqlAlterTypeBucketSelectionStrategy() {
    database.command("sql", "CREATE VERTEX TYPE Account");
    database.command("sql", "CREATE PROPERTY Account.id string");
    database.command("sql", "CREATE INDEX ON Account(id) UNIQUE");
    database.command("sql", "ALTER TYPE Account BucketSelectionStrategy `partitioned('id')`");

    final BucketSelectionStrategy strategy = database.getSchema().getType("Account").getBucketSelectionStrategy();
    assertThat(strategy.getName()).isEqualTo("partitioned");
    assertThat(((PartitionedBucketSelectionStrategy) strategy).getProperties().get(0)).isEqualTo("id");
  }

  @Test
  public void sqlAlterTypeCustom() {
    database.command("sql", "CREATE VERTEX TYPE Suv");

    database.command("sql", "ALTER TYPE Suv CUSTOM description = 'test'");
    assertThat(database.getSchema().getType("Suv").getCustomValue("description")).isEqualTo("test");

    database.command("sql", "ALTER TYPE Suv CUSTOM age = 3");
    assertThat(database.getSchema().getType("Suv").getCustomValue("age")).isEqualTo(3);

    final JSONObject cfg = database.getSchema().getEmbedded().toJSON();
    final JSONObject customMap = cfg.getJSONObject("types").getJSONObject("Suv").getJSONObject("custom");
    assertThat(customMap.getString("description")).isEqualTo("test");
    assertThat(customMap.getInt("age")).isEqualTo(3);

    database.command("sql", "ALTER TYPE Suv CUSTOM age = null");
    assertThat(database.getSchema().getType("Suv").getCustomValue("age")).isNull();
    assertThat(database.getSchema().getType("Suv").getCustomKeys().contains("age")).isFalse();
  }

  @Test
  public void sqlAlterTypeName() {
    database.command("sql", "CREATE VERTEX TYPE Mpv");
    database.command("sql", "CREATE PROPERTY Mpv.engine_number string");
    database.command("sql", "CREATE INDEX ON Mpv(engine_number) UNIQUE");

    database.begin();
    database.getSchema().getType("Mpv").newRecord()
        .set("vehicle_model", "Blista Compact")
        .set("engine_number", "456")
        .save();
    database.commit();

    Assertions.assertThrows(Exception.class, () -> {
      database.begin();
      database.getSchema().getType("Mpv").newRecord()
          .set("vehicle_model", "Maibatsu Monstrosity")
          .set("engine_number", "456")
          .save();
      database.commit();
    });

    database.command("sql", "ALTER TYPE Mpv NAME Sedan");
    Assertions.assertNotNull(database.getSchema().getType("Sedan"));
    // Assertions.assertNull fails, hence the use of database.command()
    ResultSet result = database.command("sql", "SELECT FROM schema:types");
    Assertions.assertFalse(result.stream().anyMatch(x -> x.getProperty("name").equals("Mpv")));

    database.begin();
    database.getSchema().getType("Sedan").newRecord()
        .set("engine_number", "123")
        .set("vehicle_model", "Diablo Stallion")
        .save();
    database.commit();

    Assertions.assertThrows(Exception.class, () -> {
      database.begin();
      // insert a record with the same engine_number
      database.getSchema().getType("Sedan").newRecord()
          .set("engine_number", "123")
          .set("vehicle_model", "Cartel Cruiser")
          .save();
      database.commit();
    });
  }
}
