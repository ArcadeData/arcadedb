/*
 * Copyright 2022 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import java.util.*;

public class TraversalTest {
  private static final int             SIZE = 1000000;
  private              DatabaseFactory factory;

  public static void main(String[] args) {
    new TraversalTest().test();
  }

  public void test() {
    factory = new DatabaseFactory("./target/databases/test");
    if (factory.exists())
      factory.open().drop();

    Database db = factory.create();

    db.getSchema().createVertexType("V");
    db.getSchema().createEdgeType("E");

    long begin = System.currentTimeMillis();
    RID firstRid = null;
    RID lastRid = null;
    db.begin();
    db.setWALFlush(WALFile.FLUSH_TYPE.YES_FULL);
    for (int i = 0; i < SIZE; i++) {
      MutableVertex v = db.newVertex("V");

      v.set("val", i);
      v.set("name", "vertex" + i);
      v.set("foo", "foo laksjdf lakjsdf lkasjf dlkafdjs " + i);
      v.set("bar", "foo adfakbjk lkjaw elkm,nbn apoij w.e,jr ;kjhaw erlkasjf dlkafdjs " + i);
      v.set("baz", "foo laksjdf lakjsdf .lkau s;olknawe; oih;na ero;ij; lkasjf dlkafdjs " + i);
      v.save();

      if (lastRid != null) {
        final Vertex lastV = lastRid.asVertex();
        lastV.newEdge("E", v, true);
      } else {
        firstRid = v.getIdentity();
      }

      lastRid = v.getIdentity();
    }
    db.commit();

    System.out.println("insert in " + (System.currentTimeMillis() - begin));
    db.close();

    for (int i = 0; i < 10; i++) {
      traversal(firstRid);
    }
  }

  private void traversal(RID firstRid) {
    Database db = factory.open();
    long traverseTime;

    long begin = System.currentTimeMillis();

    db.begin();

    long tot = 0;
    Vertex v = db.lookupByRID(firstRid, false).asVertex();
    tot += v.getInteger("val");

    Iterator<Vertex> vertices = v.getVertices(Vertex.DIRECTION.OUT).iterator();

    while (vertices.hasNext()) {
      v = vertices.next();
      tot += v.getInteger("val");
      vertices = v.getVertices(Vertex.DIRECTION.OUT).iterator();
    }

    db.commit();

    traverseTime = (System.currentTimeMillis() - begin);
    System.out.println("---");
    System.out.println("traverse in " + traverseTime);
    System.out.println("traverse microsec per vertex: " + (traverseTime * 1000 / SIZE));
    System.out.println("sum: " + tot);

    db.close();
  }
}
