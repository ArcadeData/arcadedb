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
package com.arcadedb.bench.workload.protocol;

import com.arcadedb.bench.server.ServerUnderTest;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * openCypher over Bolt: the Neo4j Java driver, unencrypted, the database selected per session. The BoltProtocolPlugin must be
 * enabled.
 */
public final class BoltProtocolClient implements ProtocolClient {
  private final Driver        driver;
  private final SessionConfig sessionConfig;

  public BoltProtocolClient(final ServerUnderTest server, final String database) {
    this.driver = GraphDatabase.driver("bolt://" + server.wrapper().host() + ":" + server.boltPort(),
        AuthTokens.basic(USER, ServerUnderTest.PASSWORD), Config.builder().withoutEncryption().build());
    this.sessionConfig = SessionConfig.forDatabase(database);
  }

  @Override
  public List<Map<String, Object>> query(final String cypher, final Map<String, Object> params) {
    try (final Session session = driver.session(sessionConfig)) {
      final Result result = session.run(cypher, params != null ? params : Map.of());
      final List<Map<String, Object>> rows = new ArrayList<>();
      while (result.hasNext())
        rows.add(row(result.next()));
      return rows;
    }
  }

  @Override
  public void transaction(final List<String> cyphers, final List<Map<String, Object>> params) {
    try (final Session session = driver.session(sessionConfig); final Transaction tx = session.beginTransaction()) {
      for (int i = 0; i < cyphers.size(); i++)
        tx.run(cyphers.get(i), ProtocolClient.paramsOf(params, i)).consume();
      tx.commit();
      // a transaction closed without a commit (a failed statement) is rolled back by its close
    }
  }

  private static Map<String, Object> row(final Record record) {
    final List<String> keys = record.keys();
    final Map<String, Object> row = new LinkedHashMap<>(keys.size() * 2);
    for (int i = 0; i < keys.size(); i++)
      row.put(keys.get(i), record.get(i).asObject());
    return row;
  }

  @Override
  public void close() {
    driver.close();
  }
}
