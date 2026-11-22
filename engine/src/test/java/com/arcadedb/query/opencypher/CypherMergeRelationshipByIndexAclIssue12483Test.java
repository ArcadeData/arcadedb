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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12483: the existence check of a MERGE between two bound nodes answered from an index of the relationship's type loads a
 * record only when the key exists, where the walk it replaces loads every relationship of the type on the list. For a user who may
 * not read the type the index answer would refuse only an existing key, telling it whether the type it may not read holds the value,
 * so such a user is answered by the walk: the same answer whatever the value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeRelationshipByIndexAclIssue12483Test {
  private static final String PATH = "target/databases/CypherMergeRelationshipByIndexAclIssue12483Test";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void createGraph() {
    // Without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1).createProperty("uuid", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "E", "uuid");
      database.newVertex("V").set("id", 1).save();
      database.newVertex("V").set("id", 2).save();
      database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'secret'}]->(b)").close();
    });
  }

  @AfterEach
  void dropDatabase() {
    unbind();
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  @Test
  void aUserWhoMayNotReadTheTypeIsAnsweredTheSameWhetherOrNotTheKeyExists() {
    bindUserDenyingReadsOf("E");
    final String existing = outcomeOf("secret");
    final String missing = outcomeOf("guess");
    unbind();

    assertThat(existing).isEqualTo(missing);
  }

  /** A user who may read the type is answered by the index: the existing relationship is matched, the missing one created. */
  @Test
  void aUserWhoMayReadTheTypeMergesThroughTheIndex() {
    bindUserDenyingReadsOf("Other");
    assertThat(outcomeOf("secret")).isEqualTo("ok");
    assertThat(outcomeOf("guess")).isEqualTo("ok");
    unbind();

    database.transaction(() -> {
      assertThat(database.query("opencypher", "MATCH ()-[e:E]->() RETURN count(e) AS c").next().<Number>getProperty("c").longValue())
          .isEqualTo(2L);
    });
  }

  private String outcomeOf(final String uuid) {
    try {
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: '" + uuid + "'}]->(b)").close());
      return "ok";
    } catch (final RuntimeException e) {
      if (database.isTransactionActive())
        database.rollback();
      return e.getClass().getName();
    }
  }

  private void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** Binds a user who may do anything but read the records of {@code deniedType}. */
  private void bindUserDenyingReadsOf(final String deniedType) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    if (database.getSchema().existsType(deniedType))
      deniedBucketIds.addAll(database.getSchema().getType(deniedType).getBucketIds(false));

    final SecurityDatabaseUser user = new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scoped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !deniedType.equals(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    };
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }
}
