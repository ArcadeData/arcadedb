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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12196: over gRPC, Cypher {@code MATCH (n) RETURN n} answered a vertex whose only property was {@code n}, and a column
 * that holds a record ({@code RETURN a, r, b}, SQL {@code @this}) reached the caller as a raw map. A row returning a node reads as
 * over HTTP, the node with its own properties, and a row with record columns reads as with the embedded API, records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue12196GrpcRecordRowsIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    http.command("sql", "CREATE VERTEX TYPE P12196");
    http.command("sql", "CREATE EDGE TYPE K12196");
    http.command("opencypher", "CREATE (:P12196 {name: 'a', age: 1})-[:K12196 {since: 2020}]->(:P12196 {name: 'b', age: 2})");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (http != null)
      http.close();
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void cypherReturnOfANodeIsTheNode() {
    final String query = "MATCH (n:P12196) RETURN n ORDER BY n.name";
    final List<String> rows = rows(http.query("opencypher", query));
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst()).contains("name=a").contains("age=1");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);
  }

  @Test
  void cypherColumnsHoldingElementsAreRecords() {
    // THE HTTP CLIENT HAS ITS OWN WRONG SHAPE FOR THESE COLUMNS (#12184): THE EMBEDDED API IS THE REFERENCE
    final String query = "MATCH (a:P12196)-[r:K12196]->(b:P12196) RETURN a, r, b";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("opencypher", query));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).contains("a=record:").contains("r=record:").contains("b=record:");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);
  }

  @Test
  void sqlThisColumnIsARecord() {
    final String query = "SELECT @rid, @type, @this FROM P12196 ORDER BY name";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("sql", query));
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst()).startsWith("projection{").contains("@this=record:");
    assertThat(rows(grpc.query("sql", query))).isEqualTo(rows);
  }

  /**
   * One line per row: the element it is with its properties, or the projection with every column, record values shown as
   * records.
   */
  private static List<String> rows(final ResultSet resultSet) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = resultSet) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (row.isElement()) {
          rows.add(describe(row.getElement().get()));
          continue;
        }
        final Map<String, String> columns = new TreeMap<>();
        for (final String name : row.getPropertyNames())
          columns.put(name, describe(row.getProperty(name)));
        rows.add("projection" + columns);
      }
    }
    return rows;
  }

  private static String describe(final Object value) {
    if (value instanceof Document document) {
      final Map<String, Object> properties = new TreeMap<>(document.toMap(false));
      return "record:" + document.getIdentity() + ":" + document.getTypeName() + properties;
    }
    if (value instanceof Record record)
      return "record:" + record.getIdentity();
    return String.valueOf(value);
  }
}
