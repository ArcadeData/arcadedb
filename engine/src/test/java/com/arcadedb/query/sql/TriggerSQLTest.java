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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive tests for SQL trigger support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TriggerSQLTest extends TestHelper {

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
  public void testCreateTriggerWithSQL() {
    database.command("sql",
        "CREATE TRIGGER audit_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"create\", timestamp = sysdate()'");

    Assertions.assertTrue(database.getSchema().existsTrigger("audit_trigger"));
    final Trigger trigger = database.getSchema().getTrigger("audit_trigger");
    Assertions.assertNotNull(trigger);
    Assertions.assertEquals("audit_trigger", trigger.getName());
    Assertions.assertEquals(Trigger.TriggerTiming.BEFORE, trigger.getTiming());
    Assertions.assertEquals(Trigger.TriggerEvent.CREATE, trigger.getEvent());
    Assertions.assertEquals("User", trigger.getTypeName());
    Assertions.assertEquals(Trigger.ActionType.SQL, trigger.getActionType());
  }

  @Test
  public void testCreateTriggerWithJavaScript() {
    database.command("sql",
        "CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVASCRIPT 'if (!record.email) throw new Error(\"Email required\");'");

    Assertions.assertTrue(database.getSchema().existsTrigger("validate_email"));
    final Trigger trigger = database.getSchema().getTrigger("validate_email");
    Assertions.assertNotNull(trigger);
    Assertions.assertEquals(Trigger.ActionType.JAVASCRIPT, trigger.getActionType());
  }

  @Test
  public void testCreateTriggerIfNotExists() {
    database.command("sql",
        "CREATE TRIGGER IF NOT EXISTS test_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    Assertions.assertTrue(database.getSchema().existsTrigger("test_trigger"));

    // Second call should not fail with IF NOT EXISTS
    database.command("sql",
        "CREATE TRIGGER IF NOT EXISTS test_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    Assertions.assertTrue(database.getSchema().existsTrigger("test_trigger"));
  }

  @Test
  public void testCreateTriggerDuplicate() {
    database.command("sql",
        "CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    // Should fail without IF NOT EXISTS
    Assertions.assertThrows(CommandExecutionException.class, () -> {
      database.command("sql",
          "CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User " +
              "EXECUTE SQL 'SELECT 1'");
    });
  }

  @Test
  public void testDropTrigger() {
    database.command("sql",
        "CREATE TRIGGER test_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    Assertions.assertTrue(database.getSchema().existsTrigger("test_trigger"));

    database.command("sql", "DROP TRIGGER test_trigger");

    Assertions.assertFalse(database.getSchema().existsTrigger("test_trigger"));
  }

  @Test
  public void testDropTriggerIfExists() {
    database.command("sql", "DROP TRIGGER IF EXISTS nonexistent_trigger");
    // Should not throw exception
  }

  @Test
  public void testDropTriggerNotExists() {
    Assertions.assertThrows(CommandExecutionException.class, () -> {
      database.command("sql", "DROP TRIGGER nonexistent_trigger");
    });
  }

  @Test
  public void testBeforeCreateTriggerSQL() {
    database.command("sql",
        "CREATE TRIGGER audit_create BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"before_create\", timestamp = sysdate()'");

    database.transaction(() -> {
      database.newDocument("User").set("name", "John").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_create'");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("before_create", result.next().getProperty("action"));
  }

  @Test
  public void testAfterCreateTriggerSQL() {
    database.command("sql",
        "CREATE TRIGGER audit_after AFTER CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"after_create\", timestamp = sysdate()'");

    database.transaction(() -> {
      database.newDocument("User").set("name", "Jane").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'after_create'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testBeforeUpdateTriggerSQL() {
    database.command("sql",
        "CREATE TRIGGER audit_update BEFORE UPDATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"before_update\", timestamp = sysdate()'");

    database.transaction(() -> {
      final Document user = database.newDocument("User").set("name", "Bob").save();
      user.modify().set("name", "Robert").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_update'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testBeforeDeleteTriggerSQL() {
    database.command("sql",
        "CREATE TRIGGER audit_delete BEFORE DELETE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"before_delete\", timestamp = sysdate()'");

    database.transaction(() -> {
      final Document user = database.newDocument("User").set("name", "Alice").save();
      user.delete();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'before_delete'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testBeforeCreateTriggerJavaScript() {
    // Simple JavaScript trigger that always succeeds
    database.command("sql",
        "CREATE TRIGGER js_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVASCRIPT 'true;'");

    // Should pass validation
    database.transaction(() -> {
      database.newDocument("User").set("name", "John").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'John'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testBeforeCreateTriggerAbort() {
    database.command("sql",
        "CREATE TRIGGER abort_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVASCRIPT 'false;'"); // Return false to abort

    // Record creation should be silently aborted (no exception, just not saved)
    database.transaction(() -> {
      database.newDocument("User").set("name", "Test").save();
    });

    // Verify record was not created
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'Test'");
    Assertions.assertFalse(result.hasNext());
  }

  @Test
  public void testMultipleTriggersSameType() {
    database.command("sql",
        "CREATE TRIGGER trigger1 BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"trigger1\"'");

    database.command("sql",
        "CREATE TRIGGER trigger2 BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"trigger2\"'");

    database.transaction(() -> {
      database.newDocument("User").set("name", "Test").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog ORDER BY action");
    Assertions.assertEquals(2, result.stream().count());
  }

  @Test
  public void testTriggerPersistence() {
    database.command("sql",
        "CREATE TRIGGER persistent_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'INSERT INTO AuditLog SET action = \"persistent\"'");

    Assertions.assertTrue(database.getSchema().existsTrigger("persistent_trigger"));

    // Close and reopen database
    database.close();
    reopenDatabase();

    // Trigger should still exist
    Assertions.assertTrue(database.getSchema().existsTrigger("persistent_trigger"));

    // Trigger should still work
    database.transaction(() -> {
      database.newDocument("User").set("name", "Test").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM AuditLog WHERE action = 'persistent'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testCreateTriggerOnNonExistentType() {
    Assertions.assertThrows(CommandExecutionException.class, () -> {
      database.command("sql",
          "CREATE TRIGGER bad_trigger BEFORE CREATE ON TYPE NonExistentType " +
              "EXECUTE SQL 'SELECT 1'");
    });
  }

  @Test
  public void testGetTriggers() {
    database.command("sql",
        "CREATE TRIGGER trigger1 BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    database.command("sql",
        "CREATE TRIGGER trigger2 AFTER UPDATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 2'");

    final Trigger[] triggers = database.getSchema().getTriggers();
    Assertions.assertEquals(2, triggers.length);
  }

  @Test
  public void testGetTriggersForType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product");
    });

    database.command("sql",
        "CREATE TRIGGER user_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE SQL 'SELECT 1'");

    database.command("sql",
        "CREATE TRIGGER product_trigger BEFORE CREATE ON TYPE Product " +
            "EXECUTE SQL 'SELECT 2'");

    final Trigger[] userTriggers = database.getSchema().getTriggersForType("User");
    Assertions.assertEquals(1, userTriggers.length);
    Assertions.assertEquals("user_trigger", userTriggers[0].getName());

    final Trigger[] productTriggers = database.getSchema().getTriggersForType("Product");
    Assertions.assertEquals(1, productTriggers.length);
    Assertions.assertEquals("product_trigger", productTriggers[0].getName());
  }

  @Test
  public void testAllEventTypes() {
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

    Assertions.assertEquals(8, database.getSchema().getTriggers().length);
  }

  @Test
  public void testJavaTrigger() {
    database.command("sql",
        "CREATE TRIGGER java_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaTrigger'");

    database.transaction(() -> {
      database.newDocument("User").set("name", "John").save();
    });

    // Verify the Java trigger executed and set the flag
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE triggeredByJava = true");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("John", result.next().getProperty("name"));
  }

  @Test
  public void testJavaValidationTrigger() {
    database.command("sql",
        "CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaValidationTrigger'");

    // Should fail validation
    try {
      database.transaction(() -> {
        database.newDocument("User").set("name", "NoEmail").save();
      });
      Assertions.fail("Expected validation exception");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Invalid email"));
    }

    // Should pass validation
    database.transaction(() -> {
      database.newDocument("User").set("name", "Valid", "email", "test@example.com").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE email = 'test@example.com'");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testJavaAbortTrigger() {
    database.command("sql",
        "CREATE TRIGGER abort_trigger BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaAbortTrigger'");

    // Record creation should be silently aborted
    database.transaction(() -> {
      database.newDocument("User").set("name", "Test").save();
    });

    // Verify record was not created
    final ResultSet result = database.query("sql", "SELECT FROM User WHERE name = 'Test'");
    Assertions.assertFalse(result.hasNext());
  }

  @Test
  public void testJavaTriggerPersistence() {
    database.command("sql",
        "CREATE TRIGGER persistent_java BEFORE CREATE ON TYPE User " +
            "EXECUTE JAVA 'com.arcadedb.query.sql.TestJavaTrigger'");

    Assertions.assertTrue(database.getSchema().existsTrigger("persistent_java"));

    // Close and reopen database
    database.close();
    reopenDatabase();

    // Trigger should still exist and work
    Assertions.assertTrue(database.getSchema().existsTrigger("persistent_java"));

    database.transaction(() -> {
      database.newDocument("User").set("name", "Test").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE triggeredByJava = true");
    Assertions.assertTrue(result.hasNext());
  }

  @Test
  public void testJavaTriggerInvalidClass() {
    // Should fail with class not found
    Assertions.assertThrows(Exception.class, () -> {
      database.command("sql",
          "CREATE TRIGGER bad_trigger BEFORE CREATE ON TYPE User " +
              "EXECUTE JAVA 'com.example.NonExistentClass'");
    });
  }

  @Override
  protected void reopenDatabase() {
    database = factory.open();
  }
}
