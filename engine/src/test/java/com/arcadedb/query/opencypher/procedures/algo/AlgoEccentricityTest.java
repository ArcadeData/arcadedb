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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.eccentricity Cypher procedure.
 */
class AlgoEccentricityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-eccentricity");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Linear chain A-B-C-D with BOTH direction
    // Eccentricities: A=3, B=2, C=2, D=3
    // Radius=2, Diameter=3
    // Centers: B and C (ecc==radius==2)
    // Peripheral: A and D (ecc==diameter==3)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void eccentricityReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eccentricity() YIELD node, eccentricity, isCenter, isPeripheral RETURN node, eccentricity, isCenter, isPeripheral");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void eccentricityFieldsNotNull() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eccentricity() YIELD node, eccentricity, isCenter, isPeripheral RETURN node, eccentricity, isCenter, isPeripheral");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object eccentricity = r.getProperty("eccentricity");
      final Object isCenter = r.getProperty("isCenter");
      final Object isPeripheral = r.getProperty("isPeripheral");
      assertThat(node).isNotNull();
      assertThat(eccentricity).isNotNull();
      assertThat(isCenter).isNotNull();
      assertThat(isPeripheral).isNotNull();
    }
  }

  @Test
  void eccentricityMiddleNodesAreCenters() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eccentricity() YIELD node, eccentricity, isCenter, isPeripheral " +
            "RETURN node, eccentricity, isCenter, isPeripheral");

    int eccA = 0, eccB = 0, eccC = 0, eccD = 0;
    boolean centerA = false, centerB = false, centerC = false, centerD = false;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object eccObj = r.getProperty("eccentricity");
      final Object isCenterObj = r.getProperty("isCenter");
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        final int ecc = ((Number) eccObj).intValue();
        final boolean isCenter = Boolean.TRUE.equals(isCenterObj);
        switch (name) {
          case "A" -> { eccA = ecc; centerA = isCenter; }
          case "B" -> { eccB = ecc; centerB = isCenter; }
          case "C" -> { eccC = ecc; centerC = isCenter; }
          case "D" -> { eccD = ecc; centerD = isCenter; }
        }
      }
    }

    // In a 4-node chain A-B-C-D with BOTH direction:
    // B and C have eccentricity 2 (radius), A and D have eccentricity 3 (diameter)
    assertThat(eccB).isLessThan(eccA);
    assertThat(eccC).isLessThan(eccD);
    assertThat(centerB).isTrue();
    assertThat(centerC).isTrue();
    assertThat(centerA).isFalse();
    assertThat(centerD).isFalse();
  }

  @Test
  void eccentricityEndpointsArePeripheral() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eccentricity() YIELD node, eccentricity, isCenter, isPeripheral " +
            "RETURN node, isPeripheral");

    boolean peripheralA = false, peripheralB = false, peripheralC = false, peripheralD = false;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object isPeripheralObj = r.getProperty("isPeripheral");
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        final boolean isPeripheral = Boolean.TRUE.equals(isPeripheralObj);
        switch (name) {
          case "A" -> peripheralA = isPeripheral;
          case "B" -> peripheralB = isPeripheral;
          case "C" -> peripheralC = isPeripheral;
          case "D" -> peripheralD = isPeripheral;
        }
      }
    }

    // Endpoints A and D should be peripheral (maximum eccentricity)
    assertThat(peripheralA).isTrue();
    assertThat(peripheralD).isTrue();
    // Middle nodes B and C should not be peripheral
    assertThat(peripheralB).isFalse();
    assertThat(peripheralC).isFalse();
  }

  @Test
  void eccentricityWithDirectionParameter() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eccentricity(null, 'BOTH') YIELD node, eccentricity RETURN node, eccentricity");

    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object eccObj = r.getProperty("eccentricity");
      assertThat(nodeObj).isNotNull();
      assertThat(((Number) eccObj).intValue()).isGreaterThanOrEqualTo(0);
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
