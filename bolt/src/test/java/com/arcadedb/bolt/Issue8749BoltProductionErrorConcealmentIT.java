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
import com.arcadedb.database.Database;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8749: production mode conceals the engine's error text on HTTP, gRPC, PostgreSQL, MongoDB and Gremlin, but Bolt
 * sent it in every FAILURE message, so a duplicated key handed the stored key value to any authenticated user. The Neo4j
 * status code is what a driver acts on and stays.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8749BoltProductionErrorConcealmentIT extends BaseBoltServerTest {
  private static final String TYPE   = "Unique8749";
  private static final String SECRET = "stored-secret-8749";

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

  @BeforeEach
  void createUniqueIndex() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + TYPE + " IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".k IF NOT EXISTS STRING");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON " + TYPE + " (k) UNIQUE");
    database.transaction(() -> database.newVertex(TYPE).set("k", SECRET).save());
  }

  @Test
  void autoCommitDuplicatedKeyIsConcealedInProductionMode() throws Exception {
    final BoltWireConnection.Summary failure = withMode("production", this::autoCommitDuplicate);

    assertThat(failure.code()).isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
    assertThat(failure.message()).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void failedCommitIsConcealedInProductionMode() throws Exception {
    final BoltWireConnection.Summary failure = withMode("production", this::explicitTransactionDuplicate);

    assertThat(failure.code()).isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
    assertThat(failure.message()).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void syntaxErrorKeepsItsCodeButNotItsTextInProductionMode() throws Exception {
    final BoltWireConnection.Summary failure = withMode("production", () -> {
      try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
        bolt.run("MATCH (n:" + TYPE + " {k: '" + SECRET + "'}) RETURN n ORDER BY");
        return bolt.readSummary();
      }
    });

    assertThat(failure.signature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(failure.code()).startsWith("Neo.ClientError.Statement.");
    assertThat(failure.message()).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  /** Text Bolt words itself about the request is what the client needs to fix it, and stays in production. */
  @Test
  void databaseSelectionTextIsKeptInProductionMode() throws Exception {
    final BoltWireConnection.Summary failure = withMode("production", () -> {
      try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
        bolt.run("RETURN 1", Map.of("db", "no-such-database-8749"));
        return bolt.readSummary();
      }
    });

    assertThat(failure.code()).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    assertThat(failure.message()).contains("no-such-database-8749");
  }

  @Test
  void developmentModeKeepsTheFullMessage() throws Exception {
    final BoltWireConnection.Summary autoCommit = withMode("development", this::autoCommitDuplicate);
    assertThat(autoCommit.code()).isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
    assertThat(autoCommit.message()).contains(SECRET);

    final BoltWireConnection.Summary commit = withMode("development", this::explicitTransactionDuplicate);
    assertThat(commit.message()).contains(SECRET);
  }

  /** The FAILURE of an auto-commit CREATE of a taken key: on RUN, or on the PULL that commits it. */
  private BoltWireConnection.Summary autoCommitDuplicate() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("CREATE (n:" + TYPE + " {k: '" + SECRET + "'}) RETURN n");
      BoltWireConnection.Summary summary = bolt.readSummary();
      if (summary.signature() == BoltMessage.SUCCESS) {
        bolt.pull(-1, -1);
        summary = bolt.readSummary();
      }
      assertThat(summary.signature()).isEqualTo(BoltMessage.FAILURE);
      return summary;
    }
  }

  /** The FAILURE of an explicit transaction that created a taken key, wherever it surfaces up to its COMMIT. */
  private BoltWireConnection.Summary explicitTransactionDuplicate() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.begin(getDatabaseName());
      bolt.run("CREATE (n:" + TYPE + " {k: '" + SECRET + "'})");
      BoltWireConnection.Summary summary = bolt.readSummary();
      if (summary.signature() == BoltMessage.SUCCESS) {
        bolt.pull(-1, -1);
        summary = bolt.readSummary();
      }
      if (summary.signature() == BoltMessage.SUCCESS) {
        bolt.sendNoFields(BoltMessage.COMMIT);
        summary = bolt.readSummary();
      }
      assertThat(summary.signature()).isEqualTo(BoltMessage.FAILURE);
      return summary;
    }
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }
}
