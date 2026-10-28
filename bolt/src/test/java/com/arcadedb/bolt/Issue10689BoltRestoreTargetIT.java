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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10689, through a real Bolt connection: the connection keeps the database it resolved across its requests, so one opened
 * before an {@code overwrite} restore claimed the database never meets a check made at resolution. Its writes inside the restore
 * window were acknowledged and then destroyed by the swap. They are now refused as {@code DatabaseUnavailable}, while its reads go
 * on until the swap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10689BoltRestoreTargetIT extends BaseBoltServerTest {

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
  void aConnectionOpenedBeforeTheRestoreReadsAndCannotWrite() throws Exception {
    final ArcadeDBServer server = getServer(0);
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      assertThat(write(bolt).signature()).isEqualTo(BoltMessage.SUCCESS);

      server.reserveDatabaseNameForRestore(getDatabaseName());
      try {
        assertThat(count(bolt)).as("the connection still reads the database the restore will replace").isEqualTo(1L);

        final BoltWireConnection.Summary refused = write(bolt);
        assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
        assertThat(refused.code()).isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
        assertThat(refused.message()).contains("restore");

        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
        assertThat(count(bolt)).as("nothing was written").isEqualTo(1L);
      } finally {
        server.releaseDatabaseNameReservedForRestore(getDatabaseName());
      }

      assertThat(write(bolt).signature()).as("the end of the restore gives the writes back").isEqualTo(BoltMessage.SUCCESS);
      assertThat(count(bolt)).isEqualTo(2L);
    }
  }

  /** CREATE in auto-commit, answered by the RUN or, when the RUN accepted it, by the PULL that completes it. */
  private static BoltWireConnection.Summary write(final BoltWireConnection bolt) throws Exception {
    bolt.run("CREATE (n:Issue10689Bolt)");
    final BoltWireConnection.Summary run = bolt.readSummary();
    if (run.signature() != BoltMessage.SUCCESS)
      return run;
    bolt.pull(-1, -1);
    return bolt.readSummary();
  }

  private static long count(final BoltWireConnection bolt) throws Exception {
    bolt.run("MATCH (n:Issue10689Bolt) RETURN count(n) AS c");
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    bolt.pull(-1, -1);
    final BoltWireConnection.Summary pull = bolt.readSummary();
    assertThat(pull.signature()).isEqualTo(BoltMessage.SUCCESS);
    return ((Number) ((List<?>) pull.records().get(0)).get(0)).longValue();
  }
}
