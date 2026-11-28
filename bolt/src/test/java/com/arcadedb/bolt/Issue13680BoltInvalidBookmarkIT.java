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
import com.arcadedb.bolt.message.BoltMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13680: a bookmark the server did not issue was accepted and ignored. Neo4j refuses it, on the BEGIN or the auto-commit RUN
 * that carries it, with {@code Neo.ClientError.Transaction.InvalidBookmark}.
 */
public class Issue13680BoltInvalidBookmarkIT extends BaseBoltServerTest {
  private static final String INVALID_BOOKMARK = "Neo.ClientError.Transaction.InvalidBookmark";

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

  /** On the wire: BEGIN and RUN answer InvalidBookmark, the connection is FAILED until RESET, and the same connection then serves. */
  @Test
  void aForeignBookmarkIsRefusedOnBeginAndOnRun() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      for (final String foreign : List.of("hi, this is an invalid bookmark", "neo4j:bookmark:v1:tx5", "arcade:tx:", "arcade:tx:12x", "FB:kcwQ")) {
        bolt.sendMap(BoltMessage.BEGIN, Map.of("db", getDatabaseName(), "bookmarks", List.of(foreign)));
        final BoltWireConnection.Summary begin = bolt.readSummary();
        assertThat(begin.signature()).as("BEGIN with %s", foreign).isEqualTo(BoltMessage.FAILURE);
        assertThat(begin.code()).isEqualTo(INVALID_BOOKMARK);
        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

        bolt.run("RETURN 1 AS x", Map.of("db", getDatabaseName(), "bookmarks", List.of(foreign)));
        final BoltWireConnection.Summary run = bolt.readSummary();
        assertThat(run.signature()).as("RUN with %s", foreign).isEqualTo(BoltMessage.FAILURE);
        assertThat(run.code()).isEqualTo(INVALID_BOOKMARK);
        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      }

      // One bad bookmark among good ones refuses the lot, and nothing was begun: the next RUN is a plain auto-commit one
      bolt.sendMap(BoltMessage.BEGIN, Map.of("db", getDatabaseName(), "bookmarks", List.of("arcade:tx:1", "garbage")));
      assertThat(bolt.readSummary().code()).isEqualTo(INVALID_BOOKMARK);
      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.run("RETURN 1 AS x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  /** What the server issues is accepted back: the bookmark of a COMMIT, an auto-commit bookmark, and none or an empty one. */
  @Test
  void aBookmarkTheServerIssuedIsAccepted() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.begin(getDatabaseName());
      bolt.sendNoFields(BoltMessage.COMMIT);
      final BoltWireConnection.Summary commit = bolt.readSummary();
      final Object bookmark = commit.metadata().get("bookmark");
      assertThat(bookmark).isNotNull();

      bolt.sendMap(BoltMessage.BEGIN, Map.of("db", getDatabaseName(), "bookmarks", List.of(bookmark)));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.sendNoFields(BoltMessage.ROLLBACK);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("RETURN 1 AS x", Map.of("db", getDatabaseName(), "bookmarks", List.of(bookmark, "")));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("RETURN 2 AS x", Map.of("db", getDatabaseName(), "bookmarks", List.of()));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  /** The driver's view: the refusal is a ClientException with the code, on an auto-commit run and on an explicit transaction. */
  @Test
  void theDriverSeesInvalidBookmark() {
    try (final Driver driver = driver()) {
      try (final Session session = driver.session(
          SessionConfig.builder().withDatabase(getDatabaseName()).withBookmarks(Bookmark.from("hi, this is an invalid bookmark")).build())) {
        final ClientException run = catchThrowableOfType(ClientException.class, () -> session.run("RETURN 1").consume());
        assertThat(run).isNotNull();
        assertThat(run.code()).isEqualTo(INVALID_BOOKMARK);
      }
      try (final Session session = driver.session(
          SessionConfig.builder().withDatabase(getDatabaseName()).withBookmarks(Bookmark.from("hi, this is an invalid bookmark")).build())) {
        final ClientException begin = catchThrowableOfType(ClientException.class, () -> session.beginTransaction().run("RETURN 1").consume());
        assertThat(begin).isNotNull();
        assertThat(begin.code()).isEqualTo(INVALID_BOOKMARK);
      }
      // A bookmark the server issued chains through lastBookmarks() as before
      try (final Session first = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        first.executeWrite(tx -> tx.run("CREATE (:N13680)").consume());
        final Set<Bookmark> issued = first.lastBookmarks();
        try (final Session next = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).withBookmarks(issued).build())) {
          assertThat(next.run("MATCH (n:N13680) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1L);
        }
      }
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }
}
