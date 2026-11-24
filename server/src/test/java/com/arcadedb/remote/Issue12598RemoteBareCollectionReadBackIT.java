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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12598: the links in a property the schema declares as a bare LIST or MAP (no element type) were read back by the
 * HTTP client as their "#12:0" text, and a non-finite number among the items as its marker string, while a LIST OF LINK
 * and an undeclared property (#12195) read back as RIDs.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12598RemoteBareCollectionReadBackIT extends BaseGraphServerTest {
  private RemoteDatabase db;

  @Override
  protected String getDatabaseName() {
    return "issue12598";
  }

  @BeforeEach
  void connect() {
    db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sql", "CREATE VERTEX TYPE V12598");
    db.command("sql", "CREATE PROPERTY V12598.friends LIST");
    db.command("sql", "CREATE PROPERTY V12598.byName MAP");
    db.command("sql", "CREATE PROPERTY V12598.values LIST");
    db.command("sql", "CREATE PROPERTY V12598.names LIST");
  }

  @AfterEach
  void disconnect() {
    if (db != null)
      db.close();
  }

  @Test
  void linksOfABareDeclaredListOrMapAreReadBackAsRids() {
    final RID other = db.command("sql", "INSERT INTO V12598 SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql", "INSERT INTO V12598 SET name = 'me', friends = [" + other + "], byName = {'x': " + other + "}")
        .next().getIdentity().get();

    assertLinks(db.lookupByRID(me, true).asDocument(), other);

    try (final ResultSet rs = db.query("sql", "SELECT FROM V12598 WHERE name = 'me'")) {
      assertLinks(rs.next().getElement().get(), other);
    }
    try (final ResultSet rs = db.query("opencypher", "MATCH (n:V12598 {name: 'me'}) RETURN n")) {
      assertLinks(rs.next().getVertex().get(), other);
    }
  }

  @Test
  void aNonFiniteNumberInABareDeclaredListIsReadBackAsANumber() {
    final Database server = getServerDatabase(0, getDatabaseName());
    final RID[] rid = new RID[1];
    server.transaction(() -> rid[0] = server.newVertex("V12598").set("name", "nan")
        .set("values", new ArrayList<>(List.of(Double.NaN, Double.POSITIVE_INFINITY, 1.5D))).save().getIdentity());

    final List<Object> values = db.lookupByRID(rid[0], true).asDocument().getList("values");
    assertThat(values).hasSize(3);
    assertThat(values.get(0)).isInstanceOf(Double.class);
    assertThat((Double) values.get(0)).isNaN();
    assertThat(values.get(1)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(values.get(2)).isEqualTo(1.5D);
  }

  @Test
  void textThatLooksLikeARidInABareDeclaredListStaysText() {
    final RID me = db.command("sql", "INSERT INTO V12598 SET name = 'me', names = ['#1:0', 'text']").next().getIdentity().get();
    assertThat(db.lookupByRID(me, true).asDocument().<Object>getList("names")).containsExactly("#1:0", "text");
  }

  private static void assertLinks(final Document doc, final RID other) {
    final List<Object> friends = doc.getList("friends");
    assertThat(friends).containsExactly(other);
    assertThat(friends.get(0)).isInstanceOf(RID.class);
    final Map<String, Object> byName = doc.getMap("byName");
    assertThat(byName).containsEntry("x", other);
    assertThat(byName.get("x")).isInstanceOf(RID.class);
  }
}
