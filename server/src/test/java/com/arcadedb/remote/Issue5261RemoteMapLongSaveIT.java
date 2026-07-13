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

import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
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
 * Reproduces issue #5261 (write-side follow-up to #4735): saving a remote record with a {@code MAP OF LONG} / {@code LIST OF LONG}
 * property must not fail schema validation. The remote client re-serializes the full record with {@code UPDATE ... CONTENT}, and
 * small {@code Long} values arrive as untyped JSON integers parsed as {@code Integer}; the server must coerce them back to the
 * declared nested {@code LONG} type before validation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5261RemoteMapLongSaveIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-5261";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void remoteSavePreservesNestedLongTypes() throws Exception {
    testEachServer(serverIndex -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      db.command("sqlscript", """
          create vertex type MapLongTest if not exists;
          create property MapLongTest.values if not exists MAP OF LONG;
          create property MapLongTest.numbers if not exists LIST OF LONG;
          create property MapLongTest.note if not exists STRING;
          insert into MapLongTest set values = {'first': 1001L, 'second': 2002L}, numbers = [10L, 20L, 30L], note = 'before';
          """);

      final RID rid;
      try (final ResultSet result = db.query("sql", "select from MapLongTest")) {
        assertThat(result.hasNext()).isTrue();
        final MutableVertex vertex = result.next().toElement().asVertex().modify();

        final Map<String, Object> values = (Map<String, Object>) vertex.get("values");
        assertThat(values.get("first")).isInstanceOf(Long.class);

        // Only an unrelated property is changed: RemoteDatabase.saveRecord() sends the complete record with UPDATE ... CONTENT,
        // including values/numbers, which must not be rejected by validation.
        vertex.set("note", "after");
        vertex.save();
        rid = vertex.getIdentity();
      }

      // Reload and verify the nested values are still LONG (as fixed by #4735).
      try (final ResultSet result = db.query("sql", "select from " + rid)) {
        final Result row = result.next();

        final Map<String, Object> values = row.getProperty("values");
        assertThat(values.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
        assertThat(values.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);

        final List<Object> numbers = row.getProperty("numbers");
        for (final Object item : numbers)
          assertThat(item).isInstanceOf(Long.class);
        assertThat(numbers).containsExactly(10L, 20L, 30L);

        assertThat(row.<String>getProperty("note")).isEqualTo("after");
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
