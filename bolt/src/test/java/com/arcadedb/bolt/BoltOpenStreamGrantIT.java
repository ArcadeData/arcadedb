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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A result stream a client keeps open between two PULL messages takes its next rows under the grants the user has by then: a read
 * grant removed in the meantime ends the stream at the next batch of the scan instead of letting it run on to the end of the bucket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltOpenStreamGrantIT extends BaseBoltServerTest {
  private static final String TYPE     = "BoltGrantItem";
  private static final String GROUP    = "boltGrantReaders";
  private static final String USER     = "boltGrantReader";
  private static final String PASSWORD = "boltGrantReader-secret";
  // MORE THAN THE 1,024 RECORDS A BATCH OF THE SCAN HOLDS
  private static final int    ROWS     = 3_000;
  private static final int    BATCH    = 1_024;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void anOpenStreamStopsAtTheNextBatchOnceTheReadGrantIsRemoved() {
    createRows();
    final ServerSecurity security = getServer(0).getSecurity();
    saveReaderGroup(security, true);
    createReader(security);

    try (final Driver driver = driver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final Result result = session.run("MATCH (n:" + TYPE + ") RETURN n.id AS id");
      assertThat(result.hasNext()).isTrue();
      result.next();
      int read = 1;

      saveReaderGroup(security, false);

      try {
        while (result.hasNext()) {
          result.next();
          ++read;
        }
        fail("the rest of the scan was served after the read grant was removed");
      } catch (final Neo4jException e) {
        assertThat(e.getMessage()).contains("not allowed");
      }
      assertThat(read).as("only the batch read before the grant was removed").isLessThanOrEqualTo(BATCH);
    } finally {
      security.dropUser(USER);
      security.deleteGroup(getDatabaseName(), GROUP);
    }
  }

  @Test
  void anOpenStreamIsServedInFullWhileTheReadGrantHolds() {
    createRows();
    final ServerSecurity security = getServer(0).getSecurity();
    saveReaderGroup(security, true);
    createReader(security);

    try (final Driver driver = driver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final Result result = session.run("MATCH (n:" + TYPE + ") RETURN n.id AS id");
      int read = 0;
      while (result.hasNext()) {
        result.next();
        ++read;
      }
      assertThat(read).isEqualTo(ROWS);
    } finally {
      security.dropUser(USER);
      security.deleteGroup(getDatabaseName(), GROUP);
    }
  }

  private void createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType type = database.getSchema().buildVertexType().withName(TYPE).withTotalBuckets(1).create();
    type.getOrCreateProperty("id", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex(TYPE).set("id", i).save();
    });
  }

  private void createReader(final ServerSecurity security) {
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { GROUP }))));
  }

  /** Saves the group and applies it to the users that already hold the database, as a change of the groups does. */
  private void saveReaderGroup(final ServerSecurity security, final boolean canRead) {
    final JSONArray access = canRead ? new JSONArray(new String[] { "readRecord" }) : new JSONArray();
    security.saveGroup(getDatabaseName(), GROUP, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put(TYPE, new JSONObject().put("access", access))));
    security.updateSchema((DatabaseInternal) getServerDatabase(0, getDatabaseName()));
  }

  /** A driver that pulls a few rows at a time, so the stream stays open on the server between two pulls. */
  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(USER, PASSWORD),
        Config.builder().withoutEncryption().withFetchSize(10).build());
  }
}
