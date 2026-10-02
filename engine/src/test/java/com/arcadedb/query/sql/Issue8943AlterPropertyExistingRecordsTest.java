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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8943: {@code ALTER PROPERTY ... MANDATORY/NOTNULL true} did not look at the records already
 * written, so a record without the property stayed legal, stayed out of the index, and the planner (which trusts the
 * constraint to mean "the index holds every record") silently dropped it from an index-ordered {@code ORDER BY}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8943AlterPropertyExistingRecordsTest extends TestHelper {

  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.newDocument("T").set("id", i).set("v", i).save();
      database.newDocument("T").set("id", 4).save();
    });
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      long n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      return n;
    }
  }

  @Test
  void mandatoryIsRefusedWhenARecordLacksTheProperty() {
    load();
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.v MANDATORY true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("T.v").hasMessageContaining("mandatory");
    assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isFalse();
  }

  @Test
  void notNullIsRefusedWhenARecordHoldsAnExplicitNull() {
    database.command("sql", "CREATE DOCUMENT TYPE N");
    database.command("sql", "CREATE PROPERTY N.v INTEGER");
    database.transaction(() -> database.newDocument("N").set("v", (Object) null).save());
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY N.v NOTNULL true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("cannot be null");
    assertThat(database.getSchema().getType("N").getProperty("v").isNotNull()).isFalse();
  }

  @Test
  void orderByStillReturnsEveryRecordAfterTheRefusedAlter() {
    load();
    try {
      database.command("sql", "ALTER PROPERTY T.v NOTNULL true");
      database.command("sql", "ALTER PROPERTY T.v MANDATORY true");
    } catch (final CommandExecutionException expected) {
      // refused: the constraint never takes effect
    }
    assertThat(count("SELECT id FROM T")).isEqualTo(5);
    assertThat(count("SELECT FROM T ORDER BY v")).isEqualTo(5);
  }

  @Test
  void alterIsAcceptedWhenEveryRecordConforms() {
    load();
    database.transaction(() -> database.command("sql", "UPDATE T SET v = 99 WHERE v IS NULL"));
    database.command("sql", "ALTER PROPERTY T.v NOTNULL true");
    database.command("sql", "ALTER PROPERTY T.v MANDATORY true");
    assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isTrue();
    assertThat(count("SELECT FROM T ORDER BY v")).isEqualTo(5);
  }

  @Test
  void turningTheConstraintOffIsNeverRefused() {
    load();
    database.command("sql", "ALTER PROPERTY T.v MANDATORY false");
    database.command("sql", "ALTER PROPERTY T.v NOTNULL false");
  }

  @Test
  void aSubtypeRecordIsChecked() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.v INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.transaction(() -> database.newDocument("C").set("other", 1).save());
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.v MANDATORY true"))
        .isInstanceOf(CommandExecutionException.class);
  }
}
