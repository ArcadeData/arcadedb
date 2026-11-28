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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A portal a client keeps open between two fetches takes its next rows under the grants the user has by then: a read grant removed
 * in the meantime ends the portal at the next batch of the scan instead of letting it run on to the end of the bucket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresOpenPortalGrantIT extends PostgresWireProtocolTestBase {
  private static final String TYPE     = "PortalGrantItem";
  private static final String GROUP    = "portalGrantReaders";
  private static final String USER     = "portalGrantReader";
  private static final String PASSWORD = "portalGrantReader-secret";
  // MORE THAN THE 1,024 RECORDS A BATCH OF THE SCAN HOLDS
  private static final int    ROWS     = 3_000;
  private static final int    BATCH    = 1_024;

  @Test
  void anOpenPortalStopsAtTheNextBatchOnceTheReadGrantIsRemoved() throws Exception {
    createRows();
    final ServerSecurity security = getServer(0).getSecurity();
    saveReaderGroup(security, true);
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { GROUP }))));

    try (final Connection connection = connect(USER, PASSWORD)) {
      // A cursor with a fetch size: the portal stays open between two fetches
      connection.setAutoCommit(false);
      try (final Statement statement = connection.createStatement()) {
        statement.setFetchSize(10);
        try (final ResultSet resultSet = statement.executeQuery("SELECT id FROM " + TYPE)) {
          assertThat(resultSet.next()).isTrue();
          int read = 1;

          saveReaderGroup(security, false);

          try {
            while (resultSet.next())
              ++read;
            fail("the rest of the scan was served after the read grant was removed");
          } catch (final SQLException e) {
            assertThat(e.getMessage()).contains("not allowed");
          }
          assertThat(read).as("only the batch read before the grant was removed").isLessThanOrEqualTo(BATCH);
        }
      }
    } finally {
      security.dropUser(USER);
      security.deleteGroup(getDatabaseName(), GROUP);
    }
  }

  @Test
  void anOpenPortalIsServedInFullWhileTheReadGrantHolds() throws Exception {
    createRows();
    final ServerSecurity security = getServer(0).getSecurity();
    saveReaderGroup(security, true);
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { GROUP }))));

    try (final Connection connection = connect(USER, PASSWORD)) {
      connection.setAutoCommit(false);
      try (final Statement statement = connection.createStatement()) {
        statement.setFetchSize(10);
        try (final ResultSet resultSet = statement.executeQuery("SELECT id FROM " + TYPE)) {
          int read = 0;
          while (resultSet.next())
            ++read;
          assertThat(read).isEqualTo(ROWS);
        }
      }
    } finally {
      security.dropUser(USER);
      security.deleteGroup(getDatabaseName(), GROUP);
    }
  }

  private void createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    type.getOrCreateProperty("id", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
  }

  /** Saves the group and applies it to the users that already hold the database, as a change of the groups does. */
  private void saveReaderGroup(final ServerSecurity security, final boolean canRead) {
    final JSONArray access = canRead ? new JSONArray(new String[] { "readRecord" }) : new JSONArray();
    security.saveGroup(getDatabaseName(), GROUP, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put(TYPE, new JSONObject().put("access", access))));
    security.updateSchema((DatabaseInternal) getServerDatabase(0, getDatabaseName()));
  }

  private Connection connect(final String user, final String password) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
