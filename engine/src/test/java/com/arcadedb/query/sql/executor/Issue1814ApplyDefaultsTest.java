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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for GitHub Issue #1814: SQL UPDATE not triggering schema default value.
 * Tests the APPLY DEFAULTS clause for UPDATE statements.
 */
public class Issue1814ApplyDefaultsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create type with mandatory, notnull property with default value
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.prop STRING (mandatory true, notnull true, default 'Hi')");
      database.command("sql", "CREATE PROPERTY doc.other STRING");
    });
  }

  /**
   * Reproduce the original issue: UPDATE CONTENT without the mandatory property should fail
   * when APPLY DEFAULTS is not used.
   */
  @Test
  void updateContentWithoutMandatoryPropertyFails() {
    database.transaction(() -> {
      // Insert a record with the property set
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }");

      // Verify the record was created
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE prop = 'Ho'");
      assertThat(result.hasNext()).isTrue();
    });

    // Update the record, removing the mandatory property - should fail without APPLY DEFAULTS
    assertThatThrownBy(() -> {
      database.transaction(() -> {
        database.command("sql", "UPDATE doc CONTENT { \"other\": \"new\" } WHERE prop = 'Ho'");
      });
    }).isInstanceOf(ValidationException.class)
        .hasMessageContaining("mandatory");
  }

  /**
   * Test that UPDATE CONTENT with APPLY DEFAULTS triggers default value for missing mandatory property.
   */
  @Test
  void updateContentWithApplyDefaultsSucceeds() {
    database.transaction(() -> {
      // Insert a record with the property set
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }");

      // Verify the record was created
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE prop = 'Ho'");
      assertThat(result.hasNext()).isTrue();
    });

    database.transaction(() -> {
      // Update the record with APPLY DEFAULTS - should set default value for prop
      database.command("sql", "UPDATE doc CONTENT { \"other\": \"new\" } APPLY DEFAULTS WHERE prop = 'Ho'");

      // Verify the prop was set to default value 'Hi'
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'new'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Hi");
      assertThat((String) record.getProperty("other")).isEqualTo("new");
    });
  }

  /**
   * Test that UPDATE SET with APPLY DEFAULTS also works.
   */
  @Test
  void updateSetWithApplyDefaultsSucceeds() {
    database.transaction(() -> {
      // Insert a record
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }");
    });

    database.transaction(() -> {
      // Update removing prop and applying defaults
      database.command("sql", "UPDATE doc SET prop = null, other = 'changed' APPLY DEFAULTS WHERE prop = 'Ho'");

      // Verify prop was reset to default
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'changed'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Hi");
    });
  }

  /**
   * Test that UPDATE without APPLY DEFAULTS does NOT trigger default values (preserves original behavior).
   */
  @Test
  void updateWithoutApplyDefaultsPreservesOriginalBehavior() {
    database.transaction(() -> {
      // Insert a record
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Original\", \"other\": \"value\" }");
    });

    database.transaction(() -> {
      // Update without APPLY DEFAULTS - should preserve existing value
      database.command("sql", "UPDATE doc SET other = 'changed' WHERE prop = 'Original'");

      // Verify prop was NOT changed to default
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'changed'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Original");
    });
  }

  /**
   * Test APPLY DEFAULTS with dynamic default value (sysdate).
   */
  @Test
  void updateWithApplyDefaultsAndDynamicDefault() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE timestamped");
      database.command("sql", "CREATE PROPERTY timestamped.name STRING");
      database.command("sql", "CREATE PROPERTY timestamped.updated STRING (default \"sysdate('YYYY-MM-DD')\")");
    });

    database.transaction(() -> {
      // Insert a record
      database.command("sql", "INSERT INTO timestamped SET name = 'test'");

      // Verify the default was applied
      ResultSet result = database.query("sql", "SELECT FROM timestamped WHERE name = 'test'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      String initialUpdated = record.getProperty("updated");
      assertThat((Object) initialUpdated).isNotNull();
    });

    database.transaction(() -> {
      // Update with APPLY DEFAULTS to refresh the timestamp
      database.command("sql", "UPDATE timestamped SET updated = null APPLY DEFAULTS WHERE name = 'test'");

      // Verify a new timestamp was applied
      ResultSet result = database.query("sql", "SELECT FROM timestamped WHERE name = 'test'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((Object) record.getProperty("updated")).isNotNull();
    });
  }
}
