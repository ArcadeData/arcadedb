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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12941: the body of a SQL trigger names the types it writes to and reads, and those names are type-name
 * dependants like the query of a materialized view. {@code ALTER TYPE ... NAME} of such a type re-points the body, and
 * {@code DROP TYPE} of it is refused naming the trigger; before the fix both were accepted, and every later write to the
 * trigger's own type failed with "Type with name 'Audit' was not found".
 */
class TriggerBodyTypeDependantTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE Audit");
  }

  @Test
  void renamingATypeTheBodyWritesToRepointsTheBody() {
    database.command("sql", "CREATE TRIGGER audit_a AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO Audit SET at = sysdate()'");
    insertA(1);
    assertThat(count("Audit")).isEqualTo(1);

    database.command("sql", "ALTER TYPE Audit NAME AuditLog");

    assertThat(database.getSchema().getTrigger("audit_a").getActionCode()).isEqualTo("INSERT INTO AuditLog SET at = sysdate()");
    insertA(2);
    assertThat(count("AuditLog")).isEqualTo(2);

    reopenDatabase();

    assertThat(database.getSchema().getTrigger("audit_a").getActionCode()).isEqualTo("INSERT INTO AuditLog SET at = sysdate()");
    insertA(3);
    assertThat(count("AuditLog")).as("the re-pointed body is the one persisted").isEqualTo(3);
  }

  @Test
  void droppingATypeTheBodyWritesToIsRefusedNamingTheTrigger() {
    database.command("sql", "CREATE TRIGGER audit_a AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO Audit SET at = sysdate()'");

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Audit UNSAFE")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Cannot drop type 'Audit'").hasMessageContaining("trigger 'audit_a'")
        .hasMessageContaining("DROP TRIGGER audit_a");
    assertThat(database.getSchema().existsType("Audit")).isTrue();
    insertA(1);
    assertThat(count("Audit")).isEqualTo(1);

    database.command("sql", "DROP TRIGGER audit_a");
    database.command("sql", "DROP TYPE Audit UNSAFE");
    assertThat(database.getSchema().existsType("Audit")).isFalse();
  }

  @Test
  void everyStatementFormNamingTheTypeProtectsIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.command("sql", "CREATE EDGE TYPE Link");
    final String[] bodies = { //
        "UPDATE Audit SET n = 1", //
        "DELETE FROM Audit WHERE n = 1", //
        "SELECT count(*) FROM Audit", //
        "INSERT INTO Other SET n = (SELECT count(*) FROM Audit)", //
        "INSERT INTO Other FROM SELECT FROM Audit", //
        "UPDATE Other SET n = 1 WHERE n IN (SELECT n FROM Audit)", //
        "TRUNCATE TYPE Audit UNSAFE", //
        "CREATE VERTEX Audit SET n = 1", //
        "CREATE EDGE Audit FROM $record TO $record", //
        "SELECT FROM (SELECT FROM Audit)", //
        "SELECT count(*) FROM Other LET $a = (SELECT FROM Audit)", //
        "MATCH {type: Audit, as: x} RETURN x", //
        "MATCH {types: [Other, Audit], as: x} RETURN x", //
        "MOVE VERTEX (SELECT FROM A) TO TYPE:Audit", //
        "MOVE VERTEX (SELECT FROM A) TO Audit", //
        "INSERT INTO `Audit` SET n = 1" };
    for (int i = 0; i < bodies.length; i++) {
      final String trigger = "t" + i;
      database.command("sql", "CREATE TRIGGER " + trigger + " AFTER CREATE ON TYPE A EXECUTE SQL '" + bodies[i] + "'");
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Audit UNSAFE")).as(bodies[i]).isInstanceOf(SchemaException.class)
          .hasMessageContaining("trigger '" + trigger + "'");
      database.command("sql", "DROP TRIGGER " + trigger);
    }
  }

  @Test
  void aBodyNamingTheTypeThroughAnAliasProtectsIt() {
    database.command("sql", "ALTER TYPE Audit ALIASES AuditAlias");
    database.command("sql", "CREATE TRIGGER audit_a AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO AuditAlias SET n = 1'");

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Audit UNSAFE")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("trigger 'audit_a'");

    // THE ALIAS STAYS WITH THE TYPE ACROSS A RENAME, SO THE BODY NEEDS NOTHING
    database.command("sql", "ALTER TYPE Audit NAME AuditLog");
    assertThat(database.getSchema().getTrigger("audit_a").getActionCode()).isEqualTo("INSERT INTO AuditAlias SET n = 1");
    insertA(1);
    assertThat(count("AuditLog")).isEqualTo(1);
  }

  @Test
  void aStringLiteralACommentOrAPropertyWithTheNameOfTheTypeIsNotAReference() {
    database.command("sql", "CREATE DOCUMENT TYPE Log");
    database.command("sql",
        "CREATE TRIGGER log_a AFTER CREATE ON TYPE A EXECUTE SQL \"INSERT INTO Log /* Audit */ SET note = 'Audit', Audit = 1\"");

    database.command("sql", "DROP TYPE Audit UNSAFE");

    assertThat(database.getSchema().existsType("Audit")).isFalse();
    insertA(1);
    assertThat(count("Log")).isEqualTo(1);
  }

  @Test
  void aRenameRepointsOnlyTheReferencesAndLeavesAStringLiteralAlone() {
    database.command("sql",
        "CREATE TRIGGER audit_a AFTER CREATE ON TYPE A EXECUTE SQL \"INSERT INTO Audit SET note = 'Audit', n = (SELECT count(*) FROM Audit)\"");
    // THE TARGET OF A MOVE VERTEX FOLLOWS ITS SOURCE SUBQUERY IN THE TEXT; THE TRIGGER NEVER FIRES, ONLY ITS BODY IS LOOKED AT
    database.command("sql",
        "CREATE TRIGGER move_a AFTER DELETE ON TYPE A EXECUTE SQL 'MOVE VERTEX (SELECT FROM Audit WHERE n = -1) TO TYPE:Audit'");

    database.command("sql", "ALTER TYPE Audit NAME AuditLog");

    assertThat(database.getSchema().getTrigger("audit_a").getActionCode()).isEqualTo(
        "INSERT INTO AuditLog SET note = 'Audit', n = (SELECT count(*) FROM AuditLog)");
    assertThat(database.getSchema().getTrigger("move_a").getActionCode()).isEqualTo(
        "MOVE VERTEX (SELECT FROM AuditLog WHERE n = -1) TO TYPE:AuditLog");
    insertA(1);
    try (final ResultSet rs = database.query("sql", "SELECT note FROM AuditLog")) {
      assertThat(rs.next().<String>getProperty("note")).isEqualTo("Audit");
    }
  }

  @Test
  void aRenameToANameThatNeedsQuotingQuotesItInTheBody() {
    database.command("sql", "CREATE TRIGGER audit_a AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO Audit SET n = 1'");

    database.command("sql", "ALTER TYPE Audit NAME `Audit Log`");

    assertThat(database.getSchema().getTrigger("audit_a").getActionCode()).isEqualTo("INSERT INTO `Audit Log` SET n = 1");
    insertA(1);
    assertThat(count("`Audit Log`")).isEqualTo(1);
  }

  @Test
  void aJavaScriptTriggerIsNotAnalysed() {
    database.command("sql",
        "CREATE TRIGGER js_a AFTER CREATE ON TYPE A EXECUTE JAVASCRIPT 'database.command(\"sql\", \"INSERT INTO Audit SET n = 1\");'");

    database.command("sql", "ALTER TYPE Audit NAME AuditLog");
    assertThat(database.getSchema().getTrigger("js_a").getActionCode()).contains("INSERT INTO Audit SET");

    database.command("sql", "DROP TYPE AuditLog UNSAFE");
    assertThat(database.getSchema().existsType("AuditLog")).isFalse();
  }

  @Test
  void aBodyThatDoesNotParseRefusesNothing() {
    database.command("sql", "CREATE TRIGGER broken AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO Audit SET ((('");

    database.command("sql", "ALTER TYPE Audit NAME AuditLog");
    assertThat(database.getSchema().getTrigger("broken").getActionCode()).isEqualTo("INSERT INTO Audit SET (((");

    database.command("sql", "DROP TYPE AuditLog UNSAFE");
    assertThat(database.getSchema().existsType("AuditLog")).isFalse();
  }

  /** #10207: a trigger is part of the type it is bound to, so a body naming that type does not keep the type from being dropped. */
  @Test
  void aBodyNamingItsOwnBoundTypeDoesNotBlockItsDrop() {
    database.command("sql", "CREATE TRIGGER self_a AFTER CREATE ON TYPE A EXECUTE SQL 'SELECT count(*) FROM A'");
    database.command("sql", "CREATE TRIGGER self_audit AFTER CREATE ON TYPE Audit EXECUTE SQL 'UPDATE Audit SET seen = true'");

    database.command("sql", "DROP TYPE A UNSAFE");
    database.command("sql", "DROP TYPE Audit UNSAFE");

    assertThat(database.getSchema().existsTrigger("self_a")).isFalse();
    assertThat(database.getSchema().existsTrigger("self_audit")).isFalse();
  }

  /** #10096: renaming the bound type still retargets the trigger, and a body naming the bound type follows it. */
  @Test
  void renamingTheBoundTypeRetargetsTheTriggerAndItsBody() {
    database.command("sql", "CREATE TRIGGER self_a AFTER CREATE ON TYPE A EXECUTE SQL 'INSERT INTO Audit SET n = (SELECT count(*) FROM A)'");

    database.command("sql", "ALTER TYPE A NAME B");

    assertThat(database.getSchema().getTrigger("self_a").getTypeName()).isEqualTo("B");
    assertThat(database.getSchema().getTrigger("self_a").getActionCode()).isEqualTo(
        "INSERT INTO Audit SET n = (SELECT count(*) FROM B)");
    // THE BODY RUNS ON THE NEW NAME: ON THE OLD ONE THE INSERT WOULD FAIL WITH "Type with name 'A' was not found"
    database.transaction(() -> database.command("sql", "INSERT INTO B SET x = 1"));
    assertThat(count("Audit")).isEqualTo(1);
  }

  private void insertA(final int x) {
    database.transaction(() -> database.command("sql", "INSERT INTO A SET x = " + x));
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
