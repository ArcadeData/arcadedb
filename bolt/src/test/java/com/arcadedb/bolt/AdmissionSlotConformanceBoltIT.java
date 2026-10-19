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
import com.arcadedb.database.Database;
import com.arcadedb.server.AdmissionSlotConformance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235 over BOLT: a statement that gives its admission slot back before a wait finds the slot on the thread that
 * runs it, in an auto-commit query and inside an explicit transaction. See {@link AdmissionSlotConformance}.
 */
public class AdmissionSlotConformanceBoltIT extends BaseBoltServerTest {
  private static final String TYPE = "Conformance10235";

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

  @Test
  void aStatementGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType(TYPE);

    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "Bolt auto-commit",
          () -> session.run("CREATE (n:" + TYPE + " {id: 1})").consume());

      try (final Transaction tx = session.beginTransaction()) {
        AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "Bolt explicit transaction",
            () -> tx.run("CREATE (n:" + TYPE + " {id: 2})").consume());
        tx.commit();
      }
    }
    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }
}
