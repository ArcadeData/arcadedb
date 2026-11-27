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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Property;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Declaring a column constraint on a TimeSeries type holds it against the stored samples first. That is a read of the samples, so it is
 * held to the same per-type read check every other read of them is: a TimeSeries type owns no record bucket, so the type-name check is
 * the only one it has. A user who can change the schema but cannot read the type is refused before any sample is looked at, whatever the
 * outcome of the check would have been, and a user who can read the type keeps the refusals and the acceptances it always had.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesConstraintDeclarationAccessTest {
  private static final String PATH = "target/databases/TimeSeriesConstraintDeclarationAccessTest";
  private static final String TYPE = "Readings";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (level DOUBLE, label STRING)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, host = 'a', level = 5.0, label = 'alpha'");
      database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, host = 'a', level = 50.0, label = 'beta'");
      database.command("sql", "INSERT INTO " + TYPE + " SET ts = 3000, host = 'a'");
    });
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    database.drop();
    factory.close();
  }

  @Test
  void boundsAreNotCheckedForAUserWhoCannotReadTheType() {
    bindUserWithoutReadOn(TYPE);

    // a bound some sample breaks, and one no sample breaks: both answer the same refusal, so the outcome says nothing about the samples
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level MAX 10");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level MAX 100000");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level MIN 20");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level MIN -100000");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".label REGEXP '[0-9]+'");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".label REGEXP '[a-z]+'");

    unbindUser();
    final Property level = database.getSchema().getType(TYPE).getProperty("level");
    assertThat(level.getMax()).as("a refused declaration must leave nothing declared").isNull();
    assertThat(level.getMin()).isNull();
    assertThat(database.getSchema().getType(TYPE).getProperty("label").getRegexp()).isNull();
  }

  @Test
  void existenceFlagsAreNotCheckedForAUserWhoCannotReadTheType() {
    bindUserWithoutReadOn(TYPE);

    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level MANDATORY true");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".level NOTNULL true");
    // every sample has it
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".host MANDATORY true");
    assertDeniedWithoutNamingASample("ALTER PROPERTY " + TYPE + ".host NOTNULL true");

    unbindUser();
    final Property level = database.getSchema().getType(TYPE).getProperty("level");
    assertThat(level.isMandatory()).isFalse();
    assertThat(level.isNotNull()).isFalse();
    assertThat(database.getSchema().getType(TYPE).getProperty("host").isMandatory()).isFalse();
  }

  @Test
  void existenceConstraintOfTheGraphLanguageIsNotCheckedForAUserWhoCannotReadTheType() {
    bindUserWithoutReadOn(TYPE);

    assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:" + TYPE + ") REQUIRE n.level IS NOT NULL").close())
        .isInstanceOf(SecurityException.class).hasMessageNotContaining("timestamp");

    unbindUser();
    assertThat(database.getSchema().getType(TYPE).getProperty("level").isMandatory()).isFalse();
  }

  @Test
  void aUserWhoCanReadTheTypeKeepsTheRefusalsAndTheAcceptances() {
    bindUserWithoutReadOn("SomeOtherType");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".level MAX 10"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("Cannot set MAX 10").hasMessageContaining("timestamp 2000");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".level MIN 20"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("Cannot set MIN 20").hasMessageContaining("timestamp 1000");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".label REGEXP '[0-9]+'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("REGEXP").hasMessageContaining("timestamp 1000");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".level MANDATORY true"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("Cannot set MANDATORY").hasMessageContaining("timestamp 3000");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".level NOTNULL true"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("Cannot set NOTNULL").hasMessageContaining("timestamp 3000");
    // the graph language wraps what the check refused for as the cause of its own failure
    assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:" + TYPE + ") REQUIRE n.level IS NOT NULL").close())
        .isInstanceOf(CommandExecutionException.class).cause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("timestamp 3000");

    database.command("sql", "ALTER PROPERTY " + TYPE + ".level MAX 100000").close();
    database.command("sql", "ALTER PROPERTY " + TYPE + ".host MANDATORY true").close();

    final Property level = database.getSchema().getType(TYPE).getProperty("level");
    assertThat(level.getMax()).as("an accepted declaration is applied").isEqualTo("100000");
    assertThat(level.getMin()).isNull();
    assertThat(level.isMandatory()).isFalse();
    assertThat(database.getSchema().getType(TYPE).getProperty("host").isMandatory()).isTrue();
  }

  private void assertDeniedWithoutNamingASample(final String statement) {
    assertThatThrownBy(() -> database.command("sql", statement).close()).as(statement).isInstanceOf(SecurityException.class)
        .hasMessageNotContaining("timestamp");
  }

  private void bindUserWithoutReadOn(final String typeName) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SchemaUser(typeName));
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** Grants everything, the schema changes included, except the read of the records of one type. */
  private record SchemaUser(String unreadableType) implements SecurityDatabaseUser {
    @Override
    public String getName() {
      return "schemaUser";
    }

    @Override
    public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
      return true;
    }

    @Override
    public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
      return true;
    }

    @Override
    public boolean requestAccessOnType(final String typeName, final ACCESS access) {
      return !(unreadableType.equals(typeName) && access == ACCESS.READ_RECORD);
    }

    @Override
    public long getResultSetLimit() {
      return -1L;
    }

    @Override
    public long getReadTimeout() {
      return -1L;
    }
  }
}
