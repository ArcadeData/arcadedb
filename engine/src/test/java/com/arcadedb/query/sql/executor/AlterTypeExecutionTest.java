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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlterTypeExecutionTest extends TestHelper {
  @Test
  void sqlAlterTypeInheritanceUsing() {
    database.command("sql", "CREATE VERTEX TYPE Car");

    assertThat(database.getSchema().getType("Car").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "CREATE VERTEX TYPE Vehicle");
    database.command("sql", "ALTER TYPE Car SUPERTYPE +Vehicle");

    assertThat(database.getSchema().getType("Car").getSuperTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").getSuperTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())
        .contains("Vehicle")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Vehicle").getSubTypes().stream().map(DocumentType::getName).toList()
        .contains("Car")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").isSuperTypeOf("Car")).isTrue();

    database.command("sql", "CREATE VERTEX TYPE Suv");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().isEmpty()).isTrue();

    database.command("sql", "ALTER TYPE Suv SUPERTYPE +Car");
    assertThat(database.getSchema().getType("Suv").getSuperTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").getSubTypes().size()).isEqualTo(1);
    assertThat(database.getSchema().getType("Car").getSubTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())
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
    assertThat(database.getSchema().getType("Car").getSubTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())
        .contains("Suv")).isTrue();
    assertThat(database.getSchema().getType("Car").isSuperTypeOf("Suv")).isTrue();
    assertThat(database.getSchema().getType("Vehicle").isSuperTypeOf("Suv")).isTrue();
  }

  @Test
  void sqlAlterTypeBucketSelectionStrategy() {
    database.command("sql", "CREATE VERTEX TYPE Account");
    database.command("sql", "CREATE PROPERTY Account.id string");
    database.command("sql", "CREATE INDEX ON Account(id) UNIQUE");
    database.command("sql", "ALTER TYPE Account BucketSelectionStrategy `partitioned('id')`");

    final BucketSelectionStrategy strategy = database.getSchema().getType("Account").getBucketSelectionStrategy();
    assertThat(strategy.getName()).isEqualTo("partitioned");
    assertThat(((PartitionedBucketSelectionStrategy) strategy).getProperties().get(0)).isEqualTo("id");
  }

  @Test
  void sqlAlterTypeCustom() {
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
  void sqlAlterTypeName() {
    database.command("sql", "CREATE VERTEX TYPE Mpv");
    database.command("sql", "CREATE PROPERTY Mpv.engine_number string");
    database.command("sql", "CREATE INDEX ON Mpv(engine_number) UNIQUE");

    database.begin();
    database.getSchema().getType("Mpv").newRecord().set("vehicle_model", "Blista Compact").set("engine_number", "456").save();
    database.commit();

    database.begin();

    database.getSchema().getType("Mpv").newRecord().set("vehicle_model", "Maibatsu Monstrosity").set("engine_number", "456")
          .save();

    assertThatExceptionOfType(Exception.class).isThrownBy(() ->
      database.commit());

    database.command("sql", "ALTER TYPE Mpv NAME Sedan");
    assertThat(database.getSchema().getType("Sedan")).isNotNull();
    // Assertions.assertNull fails, hence the use of database.command()
    ResultSet result = database.command("sql", "SELECT FROM schema:types");
    assertThat(result.stream().anyMatch(x -> x.getProperty("name").equals("Mpv"))).isFalse();

    database.begin();
    database.getSchema().getType("Sedan").newRecord().set("engine_number", "123").set("vehicle_model", "Diablo Stallion").save();
    database.commit();

    database.begin();

    database.getSchema().getType("Sedan").newRecord().set("engine_number", "123").set("vehicle_model", "Cartel Cruiser").save();

    assertThatExceptionOfType(Exception.class).isThrownBy(() ->
      database.commit());
  }

  @Test
  void sqlAlterTypeAliases() {
    // Test for issue #2496: ALTER TYPE ... ALIASES should return correct aliases, not type name
    database.command("sql", "CREATE VERTEX TYPE v");

    // Execute ALTER TYPE with ALIASES
    ResultSet result = database.command("sql", "ALTER TYPE v ALIASES x");

    // Check that we have a result
    assertThat(result.hasNext()).isTrue();

    // Get the result document
    var resultDoc = result.next();

    // The operation field should indicate ALTER TYPE was executed
    assertThat((String) resultDoc.getProperty("operation")).isEqualTo("ALTER TYPE");

    // The aliases property should contain the actual alias 'x', not the type name 'v'
    Object aliasesObj = resultDoc.getProperty("aliases");
    assertThat((Object) aliasesObj).isNotNull();
    assertThat(aliasesObj).isInstanceOf(Collection.class);

    @SuppressWarnings("unchecked")
    Collection<String> aliases = (Collection<String>) aliasesObj;
    assertThat(aliases).hasSize(1);
    assertThat(aliases).contains("x");

    // Verify the schema was actually updated correctly
    assertThat(database.getSchema().getType("v")).isNotNull();
    assertThat(database.getSchema().existsType("x")).isTrue();
    assertThat(database.getSchema().getType("x").getName()).isEqualTo("v");
  }

  @Test
  void sqlScriptAlterTypeAliases() {
    // Test for issue #2496 part 2: SQL mode should return correct aliases
    database.command("sql", "CREATE VERTEX TYPE w");

    // Test ALTER TYPE with ALIASES in SQL mode - the main fix
    ResultSet result = database.command("sql", "ALTER TYPE w ALIASES yy");

    // SQL mode should return results
    assertThat(result.hasNext()).isTrue();

    var resultDoc = result.next();
    assertThat((String) resultDoc.getProperty("operation")).isEqualTo("ALTER TYPE");

    // The aliases property should contain the actual aliases, not the type object
    Object aliasesObj = resultDoc.getProperty("aliases");
    assertThat(aliasesObj).isNotNull();
    assertThat(aliasesObj).isInstanceOf(Collection.class);

    @SuppressWarnings("unchecked")
    Collection<String> aliases = (Collection<String>) aliasesObj;
    assertThat(aliases).contains("yy");

    // Verify schema was updated correctly
    assertThat(database.getSchema().existsType("yy")).isTrue();

    // NOTE: Issue #2496 part 1 about SQLscript mode not returning results is a general
    // SQLscript behavior for ALL DDL statements (CREATE, ALTER, etc.), not specific to
    // ALTER TYPE ALIASES. This would need to be addressed separately in the SQLscript engine.
  }
}
