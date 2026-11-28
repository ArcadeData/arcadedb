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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11511: the summary that ends an auto-commit transaction carried no bookmark, so {@code Session.lastBookmarks()} stayed
 * empty after a bare {@code session.run(...)}; Neo4j answers one with every completed transaction, a read included. A bookmark
 * is also strictly increasing, so two transactions finishing in the same millisecond never share one.
 */
public class Issue11511BoltAutoCommitBookmarkIT extends BaseBoltServerTest {

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

  /** The driver's view: an auto-commit write and an auto-commit read each leave a bookmark in the session. */
  @Test
  void anAutoCommitQueryLeavesABookmarkInTheSession() {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.lastBookmarks()).isEmpty();

      session.run("CREATE (:N11511)").consume();
      final Set<Bookmark> afterWrite = session.lastBookmarks();
      assertThat(afterWrite).hasSize(1);

      session.run("MATCH (n:N11511) RETURN count(n) AS c").consume();
      final Set<Bookmark> afterRead = session.lastBookmarks();
      assertThat(afterRead).hasSize(1).doesNotContainAnyElementsOf(afterWrite);
    }
  }

  /** Back-to-back transactions never repeat a bookmark, whatever the clock resolution. */
  @Test
  void bookmarksOfConsecutiveTransactionsAreAllDifferent() {
    final Set<String> seen = new HashSet<>();
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      for (int i = 0; i < 200; i++) {
        session.run("RETURN 1").consume();
        for (final Bookmark bookmark : session.lastBookmarks())
          assertThat(seen.add(bookmark.value())).as("bookmark %s was handed out twice", bookmark.value()).isTrue();
      }
    }
    assertThat(seen).hasSize(200);
  }

  /** On the wire: the last SUCCESS of an auto-commit stream carries it, and that of a stream inside a transaction does not. */
  @Test
  void theBookmarkIsInTheSummaryOfAnAutoCommitStreamOnly() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("RETURN 1 AS x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final BoltWireConnection.Summary autoCommit = bolt.readSummary();
      assertThat(autoCommit.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(autoCommit.metadata()).containsKey("bookmark");
      assertThat(autoCommit.metadata().get("bookmark").toString()).startsWith("arcade:tx:");

      bolt.begin(getDatabaseName());
      bolt.run("RETURN 2 AS x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final BoltWireConnection.Summary inTransaction = bolt.readSummary();
      assertThat(inTransaction.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(inTransaction.metadata()).doesNotContainKey("bookmark");

      bolt.sendNoFields(BoltMessage.COMMIT);
      final BoltWireConnection.Summary commit = bolt.readSummary();
      assertThat(commit.metadata()).containsKey("bookmark");
      assertThat(commit.metadata().get("bookmark")).isNotEqualTo(autoCommit.metadata().get("bookmark"));
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }
}
