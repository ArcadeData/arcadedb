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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12483: a MERGE of a relationship between two bound nodes answers its existence check from an index on the relationship's
 * properties when its type has one, instead of walking an edge list. The walk read the head of that list, and any relationship a
 * concurrent transaction gave either node moved it and refused the MERGE at commit. A relationship of the same type created between
 * the same two nodes by a concurrent statement still refuses it: that statement marks the pair with the registry of #11482.
 */
class CypherMergeRelationshipByIndexIssue12483Test extends TestHelper {
  private static final String MERGE = "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: 'x'}]->(b) SET e.f = 1";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1).createProperty("uuid", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "E", "uuid");
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
    });
  }

  @Test
  void aRelationshipAConcurrentTransactionGaveAnEndpointDoesNotRefuseTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    // a relationship of node 1 to another node: its outgoing head moves
    commitFromAnotherThread("MATCH (a:V {id: 1}), (c:V {id: 3}) CREATE (a)-[:E {uuid: 'y'}]->(c)");
    database.commit();

    assertThat(count("MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")).isEqualTo(2L);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  @Test
  void theSameRelationshipCreatedConcurrentlyRefusesTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'x'}]->(b)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    // the retry finds it and does not create it again
    database.transaction(() -> database.command("opencypher", MERGE).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN e.f AS c")).isEqualTo(1L);
  }

  @Test
  void anotherRelationshipCreatedConcurrentlyBetweenTheSameNodesDoesNotRefuseTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    // the same two nodes and type, another key: a relationship the MERGE was not looking for
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'y'}]->(b)");
    database.commit();
    assertThat(count("MATCH (a:V {id: 1})-[e:E]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(2L);
  }

  @Test
  void theSameRelationshipMergedConcurrentlyRefusesTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread(MERGE);
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  @Test
  void anExistingRelationshipIsMatchedThroughTheIndexAndUpdated() {
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'x', f: 0}]->(b)")
        .close());
    database.transaction(() -> database.command("opencypher", MERGE).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN e.f AS c")).isEqualTo(1L);
    // the same key between other nodes, or the other way round, is another relationship
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 2}), (b:V {id: 1}) MERGE (a)-[e:E {uuid: 'x'}]->(b)").close());
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 3}) MERGE (a)-[e:E {uuid: 'x'}]->(b)").close());
    assertThat(count("MATCH ()-[e:E {uuid: 'x'}]->() RETURN count(e) AS c")).isEqualTo(3L);
    // an undirected MERGE matches it whichever way it points
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 3}), (b:V {id: 1}) MERGE (a)-[e:E {uuid: 'x'}]-(b)").close());
    assertThat(count("MATCH ()-[e:E {uuid: 'x'}]->() RETURN count(e) AS c")).isEqualTo(3L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void commitFromAnotherThread(final String command) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> database.command("opencypher", command).close());
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    assertThat(error.get()).isNull();
  }
}
