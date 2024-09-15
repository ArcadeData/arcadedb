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
package com.arcadedb;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

public class TransactionBucketTest extends TestHelper {
  private static final int TOT = 10000;

  @Test
  public void testPopulate() {
    // EMPTY METHOD
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);

    database.begin();

    database.scanBucket("V_0", record -> {
      assertThat(record).isNotNull();

      final Set<String> prop = new HashSet<String>();
        prop.addAll(((Document) record).getPropertyNames());

      assertThat(((Document) record).getPropertyNames().size()).isCloseTo(3, within(9));
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();
      return true;
    });

    assertThat(total.get()).isEqualTo(TOT);

    database.commit();
  }

  @Test
  public void testIterator() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    final Iterator<Record> iterator = database.iterateBucket("V_0");

    while (iterator.hasNext()) {
      final Document record = (Document) iterator.next();
      assertThat(record).isNotNull();

      final Set<String> prop = new HashSet<String>();
        prop.addAll(record.getPropertyNames());


      assertThat(record.getPropertyNames().size()).isCloseTo(3, within(9));
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();

    }

    assertThat(total.get()).isEqualTo(TOT);

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanBucket("V_0", record -> {
      final Document record2 = (Document) database.lookupByRID(record.getIdentity(), false);
      assertThat(record2).isNotNull();
      assertThat(record2).isEqualTo(record);

      final Set<String> prop = new HashSet<String>();
        prop.addAll(record2.getPropertyNames());

      assertThat(record2.getPropertyNames()).hasSize(3);
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();
      return true;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(TOT);
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() {
    final AtomicInteger total = new AtomicInteger();

    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);

    database.begin();
    try {
      database.scanBucket("V_0", record -> {
        database.deleteRecord(record);
        total.incrementAndGet();
        return true;
      });

    } finally {
      assertThat(database.countBucket("V_0")).isEqualTo(0);
    }

    database.commit();

    assertThat(total.get()).isEqualTo(TOT);

    beginTest();

    database.transaction(() -> assertThat(database.countBucket("V_0")).isEqualTo(TOT));
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    assertThatExceptionOfType(DatabaseIsReadOnlyException.class).isThrownBy(() -> {
      database.begin();

      database.scanBucket("V_0", record -> {
        database.deleteRecord(record);
        return true;
      });

      database.commit();
    });

    reopenDatabase();
  }

  @Test
  public void testIteratorOnEdges() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.getSchema().createVertexType("testIteratorOnEdges_Vertex");
    database.getSchema().createEdgeType("testIteratorOnEdges_Edge");

    final MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
    final MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
    final MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();

    database.scanType("testIteratorOnEdges_Edge", true, record -> {

      final Edge e1 = (Edge) record;
      assertThat(e1.getOut()).isEqualTo(v1.getIdentity());
      assertThat(e1.getIn()).isEqualTo(v2.getIdentity());

      total.incrementAndGet();
      return true;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(1);
  }

  @Test
  public void testScanOnEdges() {
    database.begin();

    database.getSchema().createVertexType("testIteratorOnEdges_Vertex");
    database.getSchema().createEdgeType("testIteratorOnEdges_Edge");

    final MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
    final MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
    final MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();

    final ResultSet result = database.query("sql", "select from testIteratorOnEdges_Edge");

    assertThat(result.hasNext()).isTrue();

    final Record record = result.next().getRecord().get();

    assertThat(record).isNotNull();

    final Edge e2 = (Edge) record;
    assertThat(e2.getOut()).isEqualTo(v1.getIdentity());
    assertThat(e2.getIn()).isEqualTo(v2.getIdentity());

    database.commit();
  }

  @Test
  public void testScanOnEdgesAfterTx() {
    database.transaction(() -> {
      database.getSchema().createVertexType("testIteratorOnEdges_Vertex");
      database.getSchema().createEdgeType("testIteratorOnEdges_Edge");

      final MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
      final MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
      final MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql", "select from testIteratorOnEdges_Edge");

      assertThat(result.hasNext()).isTrue();

      final Record record = result.next().getRecord().get();

      assertThat(record).isNotNull();

      final Edge e2 = (Edge) record;
      assertThat(e2.getOutVertex()).isNotNull();
      assertThat(e2.getInVertex()).isNotNull();
    });
  }

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.transaction(() -> {
      if (!database.getSchema().existsType("V"))
        database.getSchema().createDocumentType("V");

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        v.save("V_0");
      }
    });
  }
}
