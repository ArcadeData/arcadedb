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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.function.FunctionLibraryFactory;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8404: {@code DEFINE FUNCTION} and {@code DELETE FUNCTION} persisted the schema with a bare
 * {@code saveConfiguration()} instead of going through a schema recording session ({@code recordFileChanges}), so
 * under HA no {@code SCHEMA_ENTRY} was ever proposed and followers never saw the function. The recording session is
 * observable without a cluster: {@code SCHEMA_AFTER_FILE_CHANGES} fires once at the end of every outermost session,
 * and only there. The cluster-level proof is {@code Issue8404FunctionDdlReplicationIT} in the ha-raft module.
 */
class Issue8404FunctionDdlRecordsSchemaChangeTest extends TestHelper {

  private final AtomicInteger  sessions = new AtomicInteger();
  private final Callable<Void> counter  = () -> {
    sessions.incrementAndGet();
    return null;
  };

  @BeforeEach
  void registerCounter() {
    ((DatabaseInternal) database).registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
  }

  @AfterEach
  void unregisterCounter() {
    ((DatabaseInternal) database).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
  }

  @Test
  void defineFunctionInANewLibraryRunsInARecordingSession() {
    database.command("sql", "define function lib8404.one \"return 1;\" language js");

    assertThat(sessions.get()).isEqualTo(1);
    assertThat(database.getSchema().getFunctionLibrary("lib8404").hasFunction("one")).isTrue();
  }

  @Test
  void defineFunctionInAnExistingLibraryRunsInARecordingSession() {
    database.command("sql", "define function lib8404.one \"select 1 as r\" language sql");
    sessions.set(0);

    database.command("sql", "define function lib8404.two \"select 2 as r\" language sql");

    assertThat(sessions.get()).isEqualTo(1);
    assertThat(database.getSchema().getFunctionLibrary("lib8404").hasFunction("two")).isTrue();
  }

  @Test
  void deleteFunctionRunsInARecordingSession() {
    database.command("sql", "define function lib8404.one \"return 1;\" language js");
    database.command("sql", "define function lib8404.two \"return 2;\" language js");
    sessions.set(0);

    database.command("sql", "delete function lib8404.two");

    assertThat(sessions.get()).isEqualTo(1);
    assertThat(database.getSchema().getFunctionLibrary("lib8404").hasFunction("two")).isFalse();
    assertThat(database.getSchema().getFunctionLibrary("lib8404").hasFunction("one")).isTrue();
  }

  /**
   * The function is validated before the library that would hold it is published. Otherwise a broken first
   * definition leaves an empty library behind, which the schema save then persists and, under HA, replicates.
   */
  @Test
  void brokenFirstDefinitionLeavesNoEmptyLibraryBehind() {
    assertThatThrownBy(() -> database.command("sql", "define function lib8404broken.bad \"return (\" language js"))
        .isInstanceOf(FunctionExecutionException.class);

    assertThat(database.getSchema().hasFunctionLibrary("lib8404broken")).isFalse();

    // The name is still free: a valid definition creates the library as usual.
    database.command("sql", "define function lib8404broken.good \"return 2;\" language js");
    assertThat((Integer) database.getSchema().getFunction("lib8404broken", "good").execute()).isEqualTo(2);
  }

  /**
   * The caller built a library because the name was free, but another definition registered one first. Merging into
   * it could hand a function of one language to a library of another, so the late definition is refused - as
   * {@code Schema.registerFunctionLibrary} refused it before - and the winner is left untouched.
   */
  @Test
  void aLibraryThatAppearedSinceTheCallerLookedIsNotMergedInto() {
    database.command("sql", "define function lib8404race.first \"return 1;\" language js");
    final LocalSchema schema = (LocalSchema) database.getSchema();

    assertThatThrownBy(() -> schema.defineFunction("lib8404race",
        FunctionLibraryFactory.createLibrary(database, "lib8404race", "sql"),
        FunctionLibraryFactory.createFunction(database, "sql", "second", "select 2 as r", new String[] {})))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already registered");

    assertThat(schema.getFunctionLibrary("lib8404race").hasFunction("second")).isFalse();
    assertThat((Integer) schema.getFunction("lib8404race", "first").execute()).isEqualTo(1);
  }

  /**
   * {@link LocalSchema#defineFunction} is public, so it holds the {@code DEFINE FUNCTION ... LANGUAGE js} gate
   * itself: a polyglot function needs {@code UPDATE_SECURITY}, a declarative one only {@code UPDATE_SCHEMA}.
   */
  @Test
  void directDefineFunctionOfHostCodeNeedsSecurityAdmin() {
    final String path = "target/databases/test-issue8404-define-function-gate";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).setSecurity(d -> {
    }).create();
    try {
      final LocalSchema schema = db.getSchema().getEmbedded();
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath())
          .setCurrentUser(userAllowing(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA));

      assertThatThrownBy(() -> schema.defineFunction("js8404", FunctionLibraryFactory.createLibrary(db, "js8404", "js"),
          FunctionLibraryFactory.createFunction(db, "js", "f", "return 1;", new String[] {})))
          .isInstanceOf(SecurityException.class);
      assertThat(schema.hasFunctionLibrary("js8404")).isFalse();

      schema.defineFunction("sql8404", FunctionLibraryFactory.createLibrary(db, "sql8404", "sql"),
          FunctionLibraryFactory.createFunction(db, "sql", "f", "select 1 as r", new String[] {}));
      assertThat(schema.getFunctionLibrary("sql8404").hasFunction("f")).isTrue();
    } finally {
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath()).setCurrentUser(null);
      db.drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /** A principal allowed exactly {@code allowed} on the database, and everything on every file. */
  private static SecurityDatabaseUser userAllowing(final SecurityDatabaseUser.DATABASE_ACCESS allowed) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "schema-admin";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return access == allowed;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
