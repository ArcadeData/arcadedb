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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A Gremlin statement issued from a script runs as the user who issued the script: the capabilities Gremlin keeps for the server
 * administrator are checked against that user, as they are for a statement issued directly.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinFromScriptRunsAsCallerTest {
  private static final String PATH = "target/databases/GremlinFromScriptRunsAsCallerTest";

  private DatabaseFactory factory;
  private Database       database;

  @BeforeEach
  void setUp() {
    // A SECURITY MANAGER, OR THE DATABASE CHECKS NO PERMISSION AT ALL
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Item");
    database.transaction(() -> database.newVertex("Item").set("k", 1).save());
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.GREMLIN_ENGINE.reset();
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void groovyIsRefusedToTheCallerOfAScriptWhoIsNotTheServerAdministrator() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("groovy");

    bindUser(userOfTheDatabase());
    assertThatThrownBy(() -> runInScript("g.V().count()")).isInstanceOf(CommandExecutionException.class)
        .hasStackTraceContaining("reserved to the server administrator");
  }

  @Test
  void groovyStillRunsForACallerWithoutUser() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("groovy");

    assertThat(runInScript("g.V().count()")).isEqualTo(1);
  }

  @Test
  void theIoStepIsRefusedToTheCallerOfAScriptWhoIsNotTheServerAdministrator() {
    bindUser(userOfTheDatabase());
    assertThatThrownBy(() -> runInScript("g.io(\"absent.graphml\").read()")).isInstanceOf(CommandExecutionException.class)
        .hasStackTraceContaining("reserved to the server administrator");
  }

  /**
   * The Gremlin statement is issued by the script, on the thread that runs the script. Answers how many rows it returned. The
   * statement sits in a single-quoted script string, so it quotes its own strings with double quotes.
   */
  private int runInScript(final String gremlin) {
    try (final ResultSet rs = database.command("js",
        "const rs = database.command('gremlin', '" + gremlin + "'); let n = 0; while (rs.hasNext()) { rs.next(); n++; } n")) {
      return rs.next().<Number>getProperty("value").intValue();
    }
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  /** May do everything on the database, update its security included, but is not the administrator of the server. */
  private SecurityDatabaseUser userOfTheDatabase() {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scripter";
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
