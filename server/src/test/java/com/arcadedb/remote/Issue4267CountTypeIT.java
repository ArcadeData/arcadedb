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
package com.arcadedb.remote;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4267: the type returned by {@code SELECT count(*)} over the HTTP
 * RemoteDatabase must match what the gRPC client returns (both should be {@code Long}, matching
 * the engine's {@link com.arcadedb.function.sql.math.SQLFunctionCount} which produces a long).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4267CountTypeIT extends BaseGraphServerTest {

  @Override
  protected String getDatabaseName() {
    return "issue4267";
  }

  @Test
  void countStarReturnsLongOverHttp() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "create vertex type SimpleVertexEx");

    final String rid;
    database.begin();
    try {
      final MutableVertex svt1 = database.newVertex("SimpleVertexEx");
      svt1.set("svex", "svt1");
      svt1.save();
      database.commit();

      rid = svt1.getIdentity().toString();
      database.begin();
      svt1.set("svex", rid);
      svt1.save();
    } finally {
      database.commit();
    }

    final ResultSet rs = database.query("sql", "select count(*) from SimpleVertexEx where svex = ?", rid);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final String column = row.getPropertyNames().iterator().next();
    final Object value = row.getProperty(column);
    assertThat(value).isInstanceOf(Long.class);
    assertThat(((Number) value).longValue()).isEqualTo(1L);
  }

  @Test
  void aliasedCountReturnsLongOverHttp() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "create document type Doc4267");
    for (int i = 0; i < 3; i++) {
      final MutableDocument d = database.newDocument("Doc4267");
      d.set("n", i);
      d.save();
    }

    final ResultSet rs = database.query("sql", "select count(*) as c from Doc4267");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Object>getProperty("c")).isInstanceOf(Long.class);
    assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(3L);
  }
}
