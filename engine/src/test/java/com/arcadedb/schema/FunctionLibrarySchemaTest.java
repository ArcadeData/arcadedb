/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.function.FunctionLibraryFactory;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.function.sql.SQLFunctionDefinition;
import com.arcadedb.function.sql.SQLFunctionLibraryDefinition;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: function library schema.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class FunctionLibrarySchemaTest {
  /**
   * Regression for #8404: function ddl records schema change.
   *
   * Issue #8404: {@code DEFINE FUNCTION} and {@code DELETE FUNCTION} persisted the schema with a bare
   * {@code saveConfiguration()} instead of going through a schema recording session ({@code recordFileChanges}), so
   * under HA no {@code SCHEMA_ENTRY} was ever proposed and followers never saw the function. The recording session is
   * observable without a cluster: {@code SCHEMA_AFTER_FILE_CHANGES} fires once at the end of every outermost session,
   * and only there. The cluster-level proof is {@code HaSchemaDdlReplicationThreeNodeIT} in the ha-raft module.
   */
  @Nested
  class FunctionDdlRecordsSchemaChange extends TestHelper {

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
          .isInstanceOf(AlreadyExistsException.class).hasMessageContaining("already registered");

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

  /**
   * Regression for #8879: unregister function library persisted.
   *
   * Issue #8879: {@code Schema.unregisterFunctionLibrary} removed the library from memory only. It neither saved the
   * schema nor ran in a schema recording session, so the library came back on reopen and, under HA, was never removed
   * from the followers. A recording session is observable without a cluster through {@code SCHEMA_AFTER_FILE_CHANGES},
   * which fires once at the end of every outermost session; the cluster-level proof is in
   * {@code HaSchemaDdlReplicationThreeNodeIT}.
   */
  @Nested
  class UnregisterFunctionLibraryPersisted extends TestHelper {

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
    void removalSurvivesReopen() {
      database.command("sql", "DEFINE FUNCTION lib8879.f 'SELECT 1 AS result' LANGUAGE sql");
      database.command("sql", "DEFINE FUNCTION keep8879.f 'SELECT 2 AS result' LANGUAGE sql");

      database.getSchema().unregisterFunctionLibrary("lib8879");
      assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();

      reopenDatabase();

      assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();
      assertThat(database.getSchema().hasFunctionLibrary("keep8879")).isTrue();
    }

    @Test
    void removalOfAPolyglotLibrarySurvivesReopen() {
      database.command("sql", "DEFINE FUNCTION js8879.f \"return 1;\" LANGUAGE js");

      database.getSchema().unregisterFunctionLibrary("js8879");
      reopenDatabase();

      assertThat(database.getSchema().hasFunctionLibrary("js8879")).isFalse();
    }

    @Test
    void removalRunsInARecordingSession() {
      database.command("sql", "DEFINE FUNCTION lib8879.f 'SELECT 1 AS result' LANGUAGE sql");
      sessions.set(0);

      database.getSchema().unregisterFunctionLibrary("lib8879");

      assertThat(sessions.get()).isEqualTo(1);
      assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();
    }

    /**
     * A library backed by native Java code is never written to the schema file, so removing it changes nothing on disk
     * nor on any other node: it stays an in-memory removal, without a schema save or a replicated entry.
     */
    @Test
    void removalOfANativeLibraryOpensNoRecordingSession() throws Exception {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("java8879", Math.class));
      sessions.set(0);

      database.getSchema().unregisterFunctionLibrary("java8879");

      assertThat(sessions.get()).isZero();
      assertThat(database.getSchema().hasFunctionLibrary("java8879")).isFalse();
    }

    @Test
    void removalOfAnUnknownLibraryIsANoOp() {
      sessions.set(0);

      database.getSchema().unregisterFunctionLibrary("missing8879");

      assertThat(sessions.get()).isZero();
    }
  }

  /**
   * Regression for #9422: register function library persisted.
   *
   * Issue #9422: {@code Schema.registerFunctionLibrary} of a persistable (js/sql/cypher) library only put it in a map, so
   * it reached {@code schema.json} by chance and was never replicated. It now runs in a schema recording session, which is
   * observable without a cluster through {@code SCHEMA_AFTER_FILE_CHANGES}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RegisterFunctionLibraryPersisted extends TestHelper {

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
    void registeredSqlLibrarySurvivesReopenWithoutOtherDdl() {
      final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib9422");
      library.registerFunction(new SQLFunctionDefinition(database, "f", "SELECT 1 AS result"));

      database.getSchema().registerFunctionLibrary(library);
      reopenDatabase();

      assertThat(database.getSchema().hasFunctionLibrary("lib9422")).isTrue();
      assertThat(database.getSchema().getFunction("lib9422", "f")).isNotNull();
    }

    @Test
    void registrationRunsInARecordingSession() {
      sessions.set(0);

      database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422"));

      assertThat(sessions.get()).isEqualTo(1);
    }

    @Test
    void duplicateRegistrationIsRefused() {
      database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422"));

      assertThatThrownBy(() -> database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422")))
          .isInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void nativeLibraryOpensNoRecordingSession() throws Exception {
      sessions.set(0);

      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("java9422", Math.class));

      assertThat(sessions.get()).isZero();
      assertThat(database.getSchema().hasFunctionLibrary("java9422")).isTrue();
    }
  }
}
