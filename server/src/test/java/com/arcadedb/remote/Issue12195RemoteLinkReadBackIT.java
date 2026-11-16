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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12195: a LINK stored in a property the schema does not declare was read back by the HTTP client as its "#12:0" text,
 * and a list or map of links as strings, so {@code (Identifiable) doc.get(name)} failed.
 */
class Issue12195RemoteLinkReadBackIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-12195";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void linksOfAnUndeclaredPropertyAreReadBackAsRids() {
    final RemoteDatabase db = database();
    final RID other = db.command("sql", "INSERT INTO V SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql",
        "INSERT INTO V SET name = 'me', friend = " + other + ", friends = [" + other + "], byName = {'x': " + other + "}").next()
        .getIdentity().get();

    assertLinks(db.lookupByRID(me, true).asDocument(), other);

    // the same record read through a query and through an iteration
    try (final var rs = db.query("sql", "SELECT FROM V WHERE name = 'me'")) {
      assertLinks(rs.next().getElement().get(), other);
    }
    for (final Iterator<Record> it = db.iterateType("V", true); it.hasNext(); ) {
      final Document doc = it.next().asDocument();
      if ("me".equals(doc.getString("name")))
        assertLinks(doc, other);
    }
  }

  @Test
  void aProjectedListOfLinksIsAListOfRids() {
    final RemoteDatabase db = database();
    final RID other = db.command("sql", "INSERT INTO V SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql", "INSERT INTO V SET name = 'me', friend = " + other + ", friends = [" + other + "]").next()
        .getIdentity().get();

    final Result row = db.query("sql", "SELECT friend, friends FROM " + me).next();
    assertThat(row.<Object>getProperty("friend")).isEqualTo(other);
    assertThat(row.<List<Object>>getProperty("friends")).containsExactly(other);
  }

  @Test
  void aStringThatLooksLikeARidStaysAString() {
    final RemoteDatabase db = database();
    final RID me = db.command("sql", "INSERT INTO V SET name = 'me', code = '#1:0', codes = ['#1:0']").next().getIdentity().get();
    final Document doc = db.lookupByRID(me, true).asDocument();
    assertThat(doc.get("code")).isEqualTo("#1:0");
    assertThat(doc.<Object>getList("codes")).containsExactly("#1:0");
  }

  private static void assertLinks(final Document doc, final RID other) {
    assertThat(doc.get("friend")).isEqualTo(other);
    assertThat(((Identifiable) doc.get("friend")).getIdentity()).isEqualTo(other);
    assertThat(doc.<Object>getList("friends")).containsExactly(other);
    assertThat(doc.<Map<String, Object>>getMap("byName")).containsEntry("x", other);
  }

  private RemoteDatabase database() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    server.create(DATABASE_NAME);
    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sql", "CREATE VERTEX TYPE V");
    return db;
  }
}
