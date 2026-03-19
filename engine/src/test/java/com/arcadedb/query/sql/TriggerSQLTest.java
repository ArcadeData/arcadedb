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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Trigger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for SQL trigger support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TriggerSQLTest extends TestHelper {

  @BeforeEach
  public void beginTest() {
    database.transaction(() -> {
      // Clean up any leftover triggers from previous tests
      for (Trigger trigger : database.getSchema().getTriggers()) {
        database.getSchema().dropTrigger(trigger.getName());
      }

      if (!database.getSchema().existsType("User")) {
        database.getSchema().createDocumentType("User");
      }
      if (!database.getSchema().existsType("AuditLog")) {
        database.getSchema().createDocumentType("AuditLog");
      }
    });
  }

  @Test
  void createTriggerWithSQL() {
    database.command("sql",
        """
        CREATE TRIGGER audit_trigger BEFORE CREATE ON TYPE User \
        EXECUTE SQL 'INSERT INTO AuditLog SET action = "create", timestamp = sysdate()'""");

    assertThat(database.getSchema().existsTrigger("audit_trigger")).isTrue();
    final Trigger trigger = database.getSchema().getTrigger("audit_trigger");
    assertThat(trigger).isNotNull();
    assertThat(trigger.getName()).isEqualTo("audit_trigger");
    assertThat(trigger.getTiming()).isEqualTo(Trigger.TriggerTiming.BEFORE);
    assertThat(trigger.getEvent()).isEqualTo(Trigger.TriggerEvent.CREATE);
    assertThat(trigger.getTypeName()).isEqualTo("User");
    assertThat(trigger.getActionType()).isEqualTo(Trigger.ActionType.SQL);
  }

  @Test
  void createTriggerWithJavaScript() {
    database.command("sql",
        """
            CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User
            EXECUTE JAVASCRIPT 'if (!record.email) throw new Error("Email required");'""");

    assertThat(database.getSchema().existsTrigger("validate_email")).isTrue();
    final Trigger trigger = database.getSchema().getTrigger("validate_email");
    assertThat(trigger).isNotNull();
    assertThat(trigger.getActionType()).isEqualTo(Trigger.ActionType.JAVASCRIPT);
  }

  @Test
  void createTriggerIfNotExists() {
    database.command("sql",
        """
            CREATE TRIGGER IF NOT EXISTS test_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    assertThat(database.getSchema().existsTrigger("test_trigger")).isTrue();

    // Second call should not fail with IF NOT EXISTS
    database.command("sql",
        """
            CREATE TRIGGER IF NOT EXISTS test_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    assertThat(database.getSchema().existsTrigger("test_trigger")).isTrue();
  }

  @Test
  void createTriggerDuplicate() {
    database.command("sql",
        """
            CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    // Should fail without IF NOT EXISTS
    assertThatThrownBy(() -> database.command("sql",
        """
            CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'"""))
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void dropTrigger() {
    database.command("sql",
        """
            CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    assertThat(database.getSchema().existsTrigger("test_trigger")).isTrue();

    database.command("sql", "DROP TRIGGER test_trigger");

    assertThat(database.getSchema().existsTrigger("test_trigger")).isFalse();
  }

  @Test
  void dropTriggerIfExists() {
    database.command("sql", "DROP TRIGGER IF EXISTS nonexistent_trigger");
    // Should not throw exception
  }

  @Test
  void dropTriggerNotExists() {
    assertThatThrownBy(() -> database.command("sql", "DROP TRIGGER nonexistent_trigger"))
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void beforeCreateTriggerSQL() {
    database.command("sql",
        """
            CREATE TRIGGER audit_create BEFORE CREATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "before_create", timestamp = sysdate()'""");

    database.transaction(() -> database.newDocument("User").set("name", "John").save());

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_create'");
    assertThat(result.hasNext()).isTrue();
    assertThat( result.next().<String>getProperty("action")).isEqualTo("before_create");
  }

  @Test
  void afterCreateTriggerSQL() {
    database.command("sql",
        """
            CREATE TRIGGER audit_after AFTER CREATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "after_create", timestamp = sysdate()'""");

    database.transaction(() -> database.newDocument("User").set("name", "Jane").save());

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'after_create'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void beforeUpdateTriggerSQL() {
    database.command("sql",
        """
            CREATE TRIGGER audit_update BEFORE UPDATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "before_update", timestamp = sysdate()'""");

    database.transaction(() -> {
      final Document user = database.newDocument("User").set("name", "Bob").save();
      user.modify().set("name", "Robert").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_update'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void beforeDeleteTriggerSQL() {
    database.command("sql",
        """
            CREATE TRIGGER audit_delete BEFORE DELETE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "before_delete", timestamp = sysdate()'""");

    database.transaction(() -> {
      final Document user = database.newDocument("User").set("name", "Alice").save();
      user.delete();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_delete'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void beforeCreateTriggerJavaScript() {
    // Simple JavaScript trigger that always succeeds
    database.command("sql",
        """
            CREATE TRIGGER js_trigger BEFORE CREATE ON TYPE User
            EXECUTE JAVASCRIPT 'true;'""");

    // Should pass validation
    database.transaction(() -> database.newDocument("User").set("name", "John").save());

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'John'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void beforeCreateTriggerAbort() {
    database.command("sql",
        """
        CREATE TRIGGER abort_trigger BEFORE CREATE ON TYPE User \
        EXECUTE JAVASCRIPT 'false;'"""); // Return false to abort

    // Record creation should be silently aborted (no exception, just not saved)
    database.transaction(() -> database.newDocument("User").set("name", "Test").save());

    // Verify record was not created
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'Test'");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void multipleTriggersSameType() {
    database.command("sql",
        """
            CREATE TRIGGER trigger1 BEFORE CREATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "trigger1"'""");

    database.command("sql",
        """
            CREATE TRIGGER trigger2 BEFORE CREATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "trigger2"'""");

    database.transaction(() -> database.newDocument("User").set("name", "Test").save());

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog ORDER BY action");
    assertThat(result.stream().count()).isEqualTo(2);
  }

  @Test
  void triggerPersistence() {
    database.command("sql",
        """
            CREATE TRIGGER persistent_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'INSERT INTO AuditLog SET action = "persistent"'""");

    assertThat(database.getSchema().existsTrigger("persistent_trigger")).isTrue();

    // Close and reopen database
    database.close();
    reopenDatabase();

    // Trigger should still exist
    assertThat(database.getSchema().existsTrigger("persistent_trigger")).isTrue();

    // Trigger should still work
    database.transaction(() -> database.newDocument("User").set("name", "Test").save());

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'persistent'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void createTriggerOnNonExistentType() {
    assertThatThrownBy(() -> database.command("sql",
        """
            CREATE TRIGGER bad_trigger BEFORE CREATE ON TYPE NonExistentType
            EXECUTE SQL 'SELECT 1'"""))
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void getTriggers() {
    database.command("sql",
        """
            CREATE TRIGGER trigger1 BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    database.command("sql",
        """
            CREATE TRIGGER trigger2 AFTER UPDATE ON TYPE User
            EXECUTE SQL 'SELECT 2'""");

    final Trigger[] triggers = database.getSchema().getTriggers();
    assertThat(triggers).hasSize(2);
  }

  @Test
  void getTriggersForType() {
    database.transaction(() -> database.getSchema().createDocumentType("Product"));

    database.command("sql",
        """
            CREATE TRIGGER user_trigger BEFORE CREATE ON TYPE User
            EXECUTE SQL 'SELECT 1'""");

    database.command("sql",
        """
            CREATE TRIGGER product_trigger BEFORE CREATE ON TYPE Product
            EXECUTE SQL 'SELECT 2'""");

    final Trigger[] userTriggers = database.getSchema().getTriggersForType("User");
    assertThat(userTriggers).hasSize(1);
    assertThat(userTriggers[0].getName()).isEqualTo("user_trigger");

    final Trigger[] productTriggers = database.getSchema().getTriggersForType("Product");
    assertThat(productTriggers).hasSize(1);
    assertThat(productTriggers[0].getName()).isEqualTo("product_trigger");
  }

  @Test
  void allEventTypes() {
    // Test all 8 event types (BEFORE/AFTER × CREATE/READ/UPDATE/DELETE)
    database.command("sql",
        "CREATE TRIGGER t1 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
    database.command("sql",
        "CREATE TRIGGER t2 AFTER CREATE ON TYPE User EXECUTE SQL 'SELECT 2'");
    database.command("sql",
        "CREATE TRIGGER t3 BEFORE READ ON TYPE User EXECUTE SQL 'SELECT 3'");
    database.command("sql",
        "CREATE TRIGGER t4 AFTER READ ON TYPE User EXECUTE SQL 'SELECT 4'");
    database.command("sql",
        "CREATE TRIGGER t5 BEFORE UPDATE ON TYPE User EXECUTE SQL 'SELECT 5'");
    database.command("sql",
        "CREATE TRIGGER t6 AFTER UPDATE ON TYPE User EXECUTE SQL 'SELECT 6'");
    database.command("sql",
        "CREATE TRIGGER t7 BEFORE DELETE ON TYPE User EXECUTE SQL 'SELECT 7'");
    database.command("sql",
        "CREATE TRIGGER t8 AFTER DELETE ON TYPE User EXECUTE SQL 'SELECT 8'");

    assertThat(database.getSchema().getTriggers()).hasSize(8);
  }

  @Test
  void javaTrigger() {
    database.command("sql",
        """
            CREATE TRIGGER java_trigger BEFORE CREATE ON TYPE User
            EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaTrigger'""");

    database.transaction(() -> database.newDocument("User").set("name", "John").save());

    // Verify the Java trigger executed and set the flag
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE triggeredByJava = true");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("John");
  }

  @Test
  void javaValidationTrigger() {
    database.command("sql",
        """
            CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User
            EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaValidationTrigger'""");

    // Should fail validation
    assertThatThrownBy(() -> database.transaction(() ->
        database.newDocument("User").set("name", "NoEmail").save()))
        .hasMessageContaining("Invalid email");

    // Should pass validation
    database.transaction(() ->
        database.newDocument("User").set("name", "Valid", "email", "test@example.com").save());

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE email = 'test@example.com'");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void javaAbortTrigger() {
    database.command("sql",
        """
            CREATE TRIGGER abort_trigger BEFORE CREATE ON TYPE User
            EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaAbortTrigger'""");

    // Record creation should be silently aborted
    database.transaction(() -> database.newDocument("User").set("name", "Test").save());

    // Verify record was not created
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'Test'");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void javaTriggerPersistence() {
    database.command("sql",
        """
            CREATE TRIGGER persistent_java BEFORE CREATE ON TYPE User
            EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaTrigger'""");

    assertThat(database.getSchema().existsTrigger("persistent_java")).isTrue();

    // Close and reopen database
    database.close();
    reopenDatabase();

    // Trigger should still exist and work
    assertThat(database.getSchema().existsTrigger("persistent_java")).isTrue();

    database.transaction(() -> database.newDocument("User").set("name", "Test").save());

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE triggeredByJava = true");
    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void javaTriggerInvalidClass() {
    // Should fail with class not found
    assertThatThrownBy(() -> database.command("sql",
        """
            CREATE TRIGGER bad_trigger BEFORE CREATE ON TYPE User
            EXECUTE JAVA 'com.example.NonExistentClass'"""))
        .isInstanceOf(Exception.class);
  }

  @Override
  protected void reopenDatabase() {
    database = factory.open();
  }
}
