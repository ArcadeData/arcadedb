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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Execution tests for the OrientDB-compatible {@code FIND REFERENCES} command.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FindReferencesStatementExecutionTest extends TestHelper {

  @Test
  void singleRidNoReferences() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person_noref");

      final MutableDocument target = database.newDocument("Person_noref").set("name", "John");
      target.save();

      final ResultSet rs = database.query("sql", "FIND REFERENCES " + target.getIdentity());
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void singleRidDirectLink() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Owner_direct");
      database.getSchema().createDocumentType("Car_direct");

      final MutableDocument car = database.newDocument("Car_direct").set("model", "Spider");
      car.save();
      final MutableDocument owner = database.newDocument("Owner_direct").set("name", "Jack").set("car", car.getIdentity());
      owner.save();

      final ResultSet rs = database.query("sql", "FIND REFERENCES " + car.getIdentity());
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((RID) r.getProperty("rid")).isEqualTo(car.getIdentity());
      assertThat((RID) r.getProperty("referredBy")).isEqualTo(owner.getIdentity());
      final List<String> fields = r.getProperty("fields");
      assertThat(fields).contains("car.");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void linkInsideListAndMap() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Item_col");
      database.getSchema().createDocumentType("Holder_col");

      final MutableDocument item = database.newDocument("Item_col").set("v", 1);
      item.save();

      final List<RID> refs = new ArrayList<>();
      refs.add(item.getIdentity());
      final Map<String, Object> map = new HashMap<>();
      map.put("primary", item.getIdentity());

      final MutableDocument holder = database.newDocument("Holder_col").set("workers", refs).set("relations", map);
      holder.save();

      final ResultSet rs = database.query("sql", "FIND REFERENCES " + item.getIdentity());
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((RID) r.getProperty("rid")).isEqualTo(item.getIdentity());
      assertThat((RID) r.getProperty("referredBy")).isEqualTo(holder.getIdentity());
      final List<String> fields = r.getProperty("fields");
      assertThat(fields).hasSize(2);
      assertThat(fields).anyMatch(f -> f.startsWith("workers."));
      assertThat(fields).anyMatch(f -> f.startsWith("relations."));
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void restrictByClass() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Target_cls");
      database.getSchema().createDocumentType("Holder1_cls");
      database.getSchema().createDocumentType("Holder2_cls");

      final MutableDocument target = database.newDocument("Target_cls").set("v", 1);
      target.save();

      database.newDocument("Holder1_cls").set("link", target.getIdentity()).save();
      database.newDocument("Holder2_cls").set("link", target.getIdentity()).save();

      // No restriction: both holders found.
      ResultSet rs = database.query("sql", "FIND REFERENCES " + target.getIdentity());
      int total = 0;
      while (rs.hasNext()) {
        rs.next();
        total++;
      }
      rs.close();
      assertThat(total).isEqualTo(2);

      // Restricted to Holder1_cls.
      rs = database.query("sql", "FIND REFERENCES " + target.getIdentity() + " [Holder1_cls]");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(database.lookupByRID((RID) r.getProperty("referredBy"), false).asDocument().getTypeName()).isEqualTo("Holder1_cls");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void restrictByBucket() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Target_buk");
      database.getSchema().createDocumentType("Holder1_buk");
      database.getSchema().createDocumentType("Holder2_buk");

      final MutableDocument target = database.newDocument("Target_buk").set("v", 1);
      target.save();

      final MutableDocument h1 = database.newDocument("Holder1_buk").set("link", target.getIdentity());
      h1.save();
      database.newDocument("Holder2_buk").set("link", target.getIdentity()).save();

      final String h1Bucket = database.getSchema().getType("Holder1_buk").getBuckets(false).get(0).getName();

      final ResultSet rs = database.query("sql", "FIND REFERENCES " + target.getIdentity() + " [bucket:" + h1Bucket + "]");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((RID) r.getProperty("referredBy")).isEqualTo(h1.getIdentity());
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void subQueryAsTarget() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Target_sq");
      database.getSchema().createDocumentType("Holder_sq");

      final MutableDocument t1 = database.newDocument("Target_sq").set("k", "a");
      t1.save();
      final MutableDocument t2 = database.newDocument("Target_sq").set("k", "b");
      t2.save();

      database.newDocument("Holder_sq").set("link", t1.getIdentity()).save();
      database.newDocument("Holder_sq").set("link", t2.getIdentity()).save();

      final ResultSet rs = database.query("sql", "FIND REFERENCES (SELECT FROM Target_sq)");
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      rs.close();
      // Two referrers, one row each.
      assertThat(rows).isEqualTo(2);
    });
  }

  @Test
  void edgeReferencesVertex() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V_edge");
      database.getSchema().createEdgeType("E_edge");

      database.command("sql", "CREATE VERTEX V_edge SET name = 'a'").close();
      database.command("sql", "CREATE VERTEX V_edge SET name = 'b'").close();
      database.command("sql", "CREATE EDGE E_edge FROM (SELECT FROM V_edge WHERE name='a') TO (SELECT FROM V_edge WHERE name='b')").close();

      final RID bId;
      try (final ResultSet rs = database.query("sql", "SELECT FROM V_edge WHERE name='b'")) {
        bId = rs.next().getElement().get().getIdentity();
      }

      // The edge holds an `in` link to vertex `b`. FIND REFERENCES should return the edge as referrer.
      final ResultSet rs = database.query("sql", "FIND REFERENCES " + bId + " [E_edge]");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((RID) row.getProperty("rid")).isEqualTo(bId);
      // Don't assert exact field name to avoid coupling to ArcadeDB graph internals; just ensure it returns something.
      assertThat((Object) row.getProperty("referredBy")).isNotNull();
      rs.close();
    });
  }

  @Test
  void nestedEmbedded() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Target_emb");
      database.getSchema().createDocumentType("Inner_emb");
      database.getSchema().createDocumentType("Outer_emb");

      final MutableDocument target = database.newDocument("Target_emb").set("v", 1);
      target.save();

      final MutableDocument outer = database.newDocument("Outer_emb");
      outer.newEmbeddedDocument("Inner_emb", "inner").set("ref", target.getIdentity());
      outer.save();

      final ResultSet rs = database.query("sql", "FIND REFERENCES " + target.getIdentity());
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((RID) r.getProperty("referredBy")).isEqualTo(outer.getIdentity());
      final List<String> fields = r.getProperty("fields");
      assertThat(fields).anyMatch(f -> f.contains("inner.ref."));
      rs.close();
    });
  }
}
