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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The settings that decide what LOAD CSV may reach (remote URLs, the SSRF block list, local files, the import
 * directory) are security controls, so {@code ALTER DATABASE} must not let a user who only holds
 * {@code updateDatabaseSettings} change them: that permission is meant for tuning and sits below
 * {@code updateSecurity}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlterDatabaseSecuritySettingsAuthorizationTest {
  private static final String PATH = "target/databases/AlterDatabaseSecuritySettingsAuthorizationTest";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
  }

  @Test
  void blockListCannotBeClearedWithoutSecurityPermission() {
    bindUser(false);

    assertRefused("ALTER DATABASE `arcadedb.opencypher.loadCsv.blockedIpRanges` \"\"");
    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.OPENCYPHER_LOAD_CSV_BLOCKED_IP_RANGES))
        .isEqualTo(GlobalConfiguration.OPENCYPHER_LOAD_CSV_BLOCKED_IP_RANGES.getDefValue());
  }

  @Test
  void otherLoadCsvSecuritySettingsNeedSecurityPermission() {
    bindUser(false);

    assertRefused("ALTER DATABASE `arcadedb.opencypher.loadCsv.allowRemoteUrls` false");
    assertRefused("ALTER DATABASE `arcadedb.opencypher.loadCsv.allowFileUrls` true");
    assertRefused("ALTER DATABASE `arcadedb.opencypher.loadCsv.importDirectory` \"/\"");
  }

  @Test
  void securitySettingsAreAvailableWithSecurityPermission() {
    bindUser(true);

    database.command("sql", "ALTER DATABASE `arcadedb.opencypher.loadCsv.blockedIpRanges` \"\"").close();
    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.OPENCYPHER_LOAD_CSV_BLOCKED_IP_RANGES)).isEmpty();
  }

  @Test
  void tuningSettingsStayAvailableWithSettingsPermissionOnly() {
    bindUser(false);

    database.command("sql", "ALTER DATABASE `arcadedb.polyglotCommand.timeout` 12345").close();
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT)).isEqualTo(12345);
  }

  private void assertRefused(final String sql) {
    assertThatThrownBy(() -> database.command("sql", sql).close())
        .as(sql)
        .isInstanceOf(SecurityException.class);
  }

  private void bindUser(final boolean canUpdateSecurity) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "dbadmin";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return access == DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS || (canUpdateSecurity && access == DATABASE_ACCESS.UPDATE_SECURITY);
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
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
    });
  }

  private void unbindUser() {
    if (database != null && database.isOpen())
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }
}
