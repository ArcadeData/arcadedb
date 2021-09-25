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
 */
package com.arcadedb;

import com.arcadedb.database.Record;
import com.arcadedb.database.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionBucketTest extends TestHelper {
  private static final int TOT = 10000;

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanBucket("V_0", new RecordCallback() {
      @Override
      public boolean onRecord(final Record record) {
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<String>();
        for (String p : ((Document) record).getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(3, ((Document) record).getPropertyNames().size(), 9);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
        return true;
      }
    });

    Assertions.assertEquals(TOT, total.get());

    database.commit();
  }

  @Test
  public void testIterator() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    Iterator<Record> iterator = database.iterateBucket("V_0");

    while (iterator.hasNext()) {
      Document record = (Document) iterator.next();
      Assertions.assertNotNull(record);

      Set<String> prop = new HashSet<String>();
      for (String p : record.getPropertyNames())
        prop.add(p);

      Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
      Assertions.assertTrue(prop.contains("id"));
      Assertions.assertTrue(prop.contains("name"));
      Assertions.assertTrue(prop.contains("surname"));

      total.incrementAndGet();

    }

    Assertions.assertEquals(TOT, total.get());

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanBucket("V_0", new RecordCallback() {
      @Override
      public boolean onRecord(final Record record) {
        final Document record2 = (Document) database.lookupByRID(record.getIdentity(), false);
        Assertions.assertNotNull(record2);
        Assertions.assertEquals(record, record2);

        Set<String> prop = new HashSet<String>();
        for (String p : record2.getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(record2.getPropertyNames().size(), 3);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
        return true;
      }
    });

    database.commit();

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();
    try {
      database.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          database.deleteRecord(record);
          total.incrementAndGet();
          return true;
        }
      });

    } finally {
      Assertions.assertEquals(0, database.countBucket("V_0"));
    }

    database.commit();

    Assertions.assertEquals(TOT, total.get());

    beginTest();

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        Assertions.assertEquals(TOT, database.countBucket("V_0"));
      }
    });
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    Assertions.assertThrows(DatabaseIsReadOnlyException.class, () -> {
      database.begin();

      database.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          database.deleteRecord(record);
          return true;
        }
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

    MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
    MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
    MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();

    database.scanType("testIteratorOnEdges_Edge", true, new DocumentCallback() {
      @Override
      public boolean onRecord(final Document record) {

        Edge e = (Edge) record;
        Assertions.assertEquals(v1.getIdentity(), e.getOut());
        Assertions.assertEquals(v2.getIdentity(), e.getIn());

        total.incrementAndGet();
        return true;
      }
    });

    database.commit();

    Assertions.assertEquals(1, total.get());
  }

  @Test
  public void testScanOnEdges() {
    database.begin();

    database.getSchema().createVertexType("testIteratorOnEdges_Vertex");
    database.getSchema().createEdgeType("testIteratorOnEdges_Edge");

    MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
    MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
    MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();

    final ResultSet result = database.query("sql", "select from testIteratorOnEdges_Edge");

    Assertions.assertTrue(result.hasNext());

    final Record record = result.next().getRecord().get();

    Assertions.assertNotNull(record);

    Edge e2 = (Edge) record;
    Assertions.assertEquals(v1.getIdentity(), e2.getOut());
    Assertions.assertEquals(v2.getIdentity(), e2.getIn());

    database.commit();
  }

  @Test
  public void testScanOnEdgesAfterTx() {
    database.transaction((tx) -> {
      database.getSchema().createVertexType("testIteratorOnEdges_Vertex");
      database.getSchema().createEdgeType("testIteratorOnEdges_Edge");

      MutableVertex v1 = database.newVertex("testIteratorOnEdges_Vertex").save();
      MutableVertex v2 = database.newVertex("testIteratorOnEdges_Vertex").save();
      MutableEdge e = v1.newEdge("testIteratorOnEdges_Edge", v2, true).save();
    });


    database.transaction((tx) -> {
      final ResultSet result = database.query("sql", "select from testIteratorOnEdges_Edge");

      Assertions.assertTrue(result.hasNext());

      final Record record = result.next().getRecord().get();

      Assertions.assertNotNull(record);

      Edge e2 = (Edge) record;
      Assertions.assertNotNull(e2.getOutVertex());
      Assertions.assertNotNull(e2.getInVertex());
    });
  }

  @Override
  protected void beginTest() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save("V_0");
        }
      }
    });
  }
}
