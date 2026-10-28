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
import com.arcadedb.schema.Trigger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10750: a trigger created ON one of a type's ALIASES stored the alias, so the cleanup of {@code DROP TYPE}, which
 * compares the type's own name, left it behind; the orphan then bound to, and fired on, an unrelated type created later under
 * the alias name. {@code getTriggersForType} of the type's own name did not list it either. A trigger now names the type itself
 * whichever of its names the statement used.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10750TriggerOnAliasTest extends TestHelper {

  @Test
  void triggerCreatedOnAnAliasNamesTheTypeItself() {
    database.command("sql", "CREATE DOCUMENT TYPE Secret");
    database.command("sql", "ALTER TYPE Secret ALIASES Shadow");
    database.command("sql", "CREATE TRIGGER t1 BEFORE CREATE ON Shadow EXECUTE SQL 'SELECT 1'");

    assertThat(database.getSchema().getTrigger("t1").getTypeName()).isEqualTo("Secret");
    assertThat(database.getSchema().getTriggersForType("Secret")).extracting(Trigger::getName).containsExactly("t1");
    assertThat(database.getSchema().getTriggersForType("Shadow")).extracting(Trigger::getName).containsExactly("t1");
  }

  @Test
  void dropTypeRemovesTheTriggerNamedByAnAlias() {
    database.command("sql", "CREATE DOCUMENT TYPE Secret");
    database.command("sql", "ALTER TYPE Secret ALIASES Shadow");
    database.command("sql", "CREATE DOCUMENT TYPE Audit");
    database.command("sql", "CREATE TRIGGER t1 BEFORE CREATE ON Shadow EXECUTE SQL 'INSERT INTO Audit SET x = 1'");

    database.command("sql", "DROP TYPE Secret");
    assertThat(database.getSchema().existsTrigger("t1")).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().existsTrigger("t1")).isFalse();

    // A BRAND NEW TYPE UNDER THE OLD ALIAS NAME INHERITS NOTHING, NOT EVEN AFTER A REOPEN
    database.command("sql", "CREATE DOCUMENT TYPE Shadow");
    reopenDatabase();
    assertThat(database.getSchema().getTriggersForType("Shadow")).isEmpty();
    database.transaction(() -> database.command("sql", "INSERT INTO Shadow SET y = 1"));
    assertThat(database.countType("Audit", false)).isZero();
  }

  @Test
  void triggerSurvivesTheRemovalOfTheAliasItWasCreatedWith() {
    database.command("sql", "CREATE DOCUMENT TYPE Secret");
    database.command("sql", "ALTER TYPE Secret ALIASES Shadow");
    database.command("sql", "CREATE DOCUMENT TYPE Audit");
    database.command("sql", "CREATE TRIGGER t1 AFTER CREATE ON Shadow EXECUTE SQL 'INSERT INTO Audit SET x = 1'");

    // THE ALIAS GOES, THE TRIGGER STAYS BOUND TO ITS TYPE ACROSS A REOPEN
    database.command("sql", "ALTER TYPE Secret ALIASES NULL");
    reopenDatabase();

    assertThat(database.getSchema().getTriggersForType("Secret")).extracting(Trigger::getName).containsExactly("t1");
    database.transaction(() -> database.command("sql", "INSERT INTO Secret SET y = 1"));
    assertThat(database.countType("Audit", false)).isEqualTo(1);
  }

  @Test
  void renameCarriesToATriggerCreatedOnAnAlias() {
    database.command("sql", "CREATE DOCUMENT TYPE Secret");
    database.command("sql", "ALTER TYPE Secret ALIASES Shadow");
    database.command("sql", "CREATE TRIGGER t1 BEFORE CREATE ON Shadow EXECUTE SQL 'SELECT 1'");

    database.command("sql", "ALTER TYPE Secret NAME Vault");
    assertThat(database.getSchema().getTrigger("t1").getTypeName()).isEqualTo("Vault");

    database.command("sql", "DROP TYPE Vault");
    assertThat(database.getSchema().existsTrigger("t1")).isFalse();
  }
}
