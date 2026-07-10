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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #4735: a property declared as {@code MAP LONG} (or {@code LIST LONG}) returns the nested values as
 * {@link Integer} instead of {@link Long} through the remote Java client when the numeric value fits in the integer range.
 */
class Issue4735MapLongIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-4735";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void mapLongValuesAreHydratedAsLong() throws Exception {
    testEachServer(serverIndex -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      db.command("sqlscript", """
          create vertex type MapLongTest if not exists;
          create property MapLongTest.values if not exists MAP OF LONG;
          create property MapLongTest.list if not exists LIST OF LONG;
          insert into MapLongTest set values = {'first': 1001L, 'second': 2002L}, list = [10L, 20L, 30L];
          """);

      try (final ResultSet rs = db.query("sql", "select from MapLongTest")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();

        final Map<String, Object> values = row.getProperty("values");
        assertThat(values.get("first")).isInstanceOf(Long.class);
        assertThat(values.get("second")).isInstanceOf(Long.class);
        assertThat(values.get("first")).isEqualTo(1001L);
        assertThat(values.get("second")).isEqualTo(2002L);

        final List<Object> list = row.getProperty("list");
        for (final Object item : list)
          assertThat(item).isInstanceOf(Long.class);
        assertThat(list).containsExactly(10L, 20L, 30L);
      }
    });
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
