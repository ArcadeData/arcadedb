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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8635 on the leader of a Raft cluster: a DDL commits its internal transactions (the dictionary entry for a new
 * name, the index build) through {@code RaftReplicatedDatabase.commit()}, not through {@code LocalDatabase.commit()},
 * and those commit paths used to write {@code schema.json} on their own. They now leave the write to the DDL, which
 * writes it once - or, inside a transaction, to the end of the transaction.
 * <p>
 * Counted with {@link LocalSchema#getVersion()}, which moves once per write of {@code schema.json}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8635SchemaSaveOncePerDdlHATest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void theLeaderWritesTheSchemaOncePerDdlAndOncePerTransaction() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final LocalSchema schema = leader.getSchema().getEmbedded();

    long before = schema.getVersion();
    leader.getSchema().createDocumentType("Issue8635Doc");
    assertThat(schema.getVersion() - before).as("CREATE TYPE").isEqualTo(1);

    before = schema.getVersion();
    leader.getSchema().getType("Issue8635Doc").createProperty("neverSeenBefore8635", Type.LONG);
    assertThat(schema.getVersion() - before).as("CREATE PROPERTY with a name new to the dictionary").isEqualTo(1);

    before = schema.getVersion();
    leader.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Issue8635Doc", "neverSeenBefore8635");
    assertThat(schema.getVersion() - before).as("CREATE INDEX").isEqualTo(1);

    leader.begin();
    before = schema.getVersion();
    for (int i = 0; i < 3; i++) {
      leader.getSchema().createDocumentType("Issue8635Tx" + i).createProperty("k" + i, Type.LONG);
      leader.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Issue8635Tx" + i, "k" + i);
    }
    assertThat(schema.getVersion()).as("nothing written while the transaction is open").isEqualTo(before);
    leader.commit();
    assertThat(schema.getVersion() - before).as("one write at the commit").isEqualTo(1);
    assertThat(schema.isDirty()).isFalse();

    // A NESTED TRANSACTION COMMITTED UNDER AN OPEN ONE THAT RAN DDL: THE RAFT COMMIT PATH ASKS FOR THE SAVE BEFORE THE
    // NESTED CONTEXT LEFT THE STACK, WHERE isTransactionActive() ALREADY ANSWERS false. THE ENCLOSING END WRITES IT
    leader.begin();
    before = schema.getVersion();
    leader.getSchema().createDocumentType("Issue8635Nested");
    leader.begin();
    leader.newDocument("Issue8635Doc").set("neverSeenBefore8635", 1L).set("anotherNewName8635", 2L).save();
    leader.commit();
    assertThat(schema.getVersion()).as("the nested commit leaves the write to the enclosing transaction").isEqualTo(before);
    leader.commit();
    assertThat(schema.getVersion() - before).as("one write at the enclosing commit").isEqualTo(1);

    assertClusterConsistency();

    for (int server = 0; server < getServerCount(); server++) {
      final Schema serverSchema = getServerDatabase(server, getDatabaseName()).getSchema();
      assertThat(serverSchema.getType("Issue8635Doc").getAllIndexes(false)).as("server %d", server).hasSize(1);
      for (int i = 0; i < 3; i++)
        assertThat(serverSchema.getType("Issue8635Tx" + i).getAllIndexes(false)).as("server %d", server).hasSize(1);
      assertThat(serverSchema.existsType("Issue8635Nested")).as("server %d", server).isTrue();
    }
  }
}
