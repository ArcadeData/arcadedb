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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for issue #7874, driven over the wire because the subject is what a Neo4j driver
 * actually receives.
 * <p>
 * The unit test beside it pins the classification; this one pins that the classification is REACHED - that
 * {@code ensureDatabase()} consults it on both of the paths that call it, RUN and BEGIN. The two halves are
 * separate deliberately: the neighbouring bug, issue #7915, was precisely a correct classifier that two handlers
 * never asked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7874BoltDatabaseNotFoundIT extends BaseBoltServerTest {

  private static final String MISSING_DATABASE = "issue7874-no-such-database";

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

  /**
   * The reported case: a typo in the {@code database} connection parameter. It used to answer
   * {@code Neo.DatabaseError.General.UnknownError}, which a driver logs as an internal server fault and an
   * application cannot tell from a broken database.
   */
  @Test
  void runNamingADatabaseThatDoesNotExistIsAClientError() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("RETURN 1", Map.of("db", MISSING_DATABASE));

      final BoltWireConnection.Summary failure = bolt.readSummary();
      assertThat(failure.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(failure.code()).isEqualTo("Neo.ClientError.Database.DatabaseNotFound");
      assertThat(failure.code()).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
      assertThat(failure.message()).contains(MISSING_DATABASE);
    }
  }

  /**
   * BEGIN resolves the database through the very same method, so it has to answer the same thing. A client that
   * opens an explicit transaction is the case where the generic error hurt most: a managed transaction cannot
   * distinguish it from a broken database and has no basis on which to stop retrying.
   */
  @Test
  void beginNamingADatabaseThatDoesNotExistIsTheSameClientError() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.sendBegin(MISSING_DATABASE);

      final BoltWireConnection.Summary failure = bolt.readSummary();
      assertThat(failure.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(failure.code()).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    }
  }

  /**
   * The refusal must not cost the connection its ability to work against a database that IS there: the failure
   * leaves no half-resolved handle behind, so a RESET and a correctly named RUN still succeed.
   */
  @Test
  void theConnectionStillWorksAgainstARealDatabaseAfterTheRefusal() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("RETURN 1", Map.of("db", MISSING_DATABASE));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.FAILURE);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("MATCH (n) RETURN count(n) AS total", Map.of("db", getDatabaseName()));
      assertThat(bolt.readSummary().signature())
          .as("a refused database selection must not poison the connection").isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }
}
