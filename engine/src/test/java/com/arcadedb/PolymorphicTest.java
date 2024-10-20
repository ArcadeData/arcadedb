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

import com.arcadedb.database.Database;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.VertexType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PolymorphicTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      //------------------------------------------------------------------------------------------------------------------------------------
      // Vehicle <|---- Motorcycle
      //         <|---- Car <|---- Supercar
      //------------------------------------------------------------------------------------------------------------------------------------
      final VertexType vehicle = database.getSchema().buildVertexType().withName("Vehicle").withTotalBuckets(3).create();
      vehicle.createProperty("brand", String.class);

      final VertexType motorcycle = database.getSchema().buildVertexType().withName("Motorcycle").withTotalBuckets(3).create();
      motorcycle.addSuperType("Vehicle");

      try {
        motorcycle.createProperty("brand", String.class);
        fail("Expected to fail by creating the same property name as the parent type");
      } catch (final SchemaException e) {
      }

      assertThat(database.getSchema().getType("Motorcycle").instanceOf("Vehicle")).isTrue();

      database.getSchema().buildVertexType().withName("Car").withTotalBuckets(3).create().addSuperType("Vehicle");
      assertThat(database.getSchema().getType("Car").instanceOf("Vehicle")).isTrue();

      database.getSchema().buildVertexType().withName("Supercar").withTotalBuckets(3).create().addSuperType("Car");
      assertThat(database.getSchema().getType("Supercar").instanceOf("Car")).isTrue();
      assertThat(database.getSchema().getType("Supercar").instanceOf("Vehicle")).isTrue();

      //------------
      // PEOPLE VERTICES
      //------------
      final VertexType person = database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Client").addSuperType(person);
      assertThat(database.getSchema().getType("Client").instanceOf("Person")).isTrue();
      assertThat(database.getSchema().getType("Client").instanceOf("Vehicle")).isFalse();

      //------------
      // EDGES
      //------------
      database.getSchema().createEdgeType("Drives");
      database.getSchema().createEdgeType("Owns").addSuperType("Drives");

      assertThat(database.getSchema().getType("Owns").instanceOf("Drives")).isTrue();
      assertThat(database.getSchema().getType("Owns").instanceOf("Vehicle")).isFalse();
    });

    final Database db = database;
    db.begin();

    final MutableVertex maserati = db.newVertex("Car");
    maserati.set("brand", "Maserati");
    maserati.set("type", "Ghibli");
    maserati.set("year", 2017);
    maserati.save();

    final MutableVertex ducati = db.newVertex("Motorcycle");
    ducati.set("brand", "Ducati");
    ducati.set("type", "Monster");
    ducati.set("year", 2015);
    ducati.save();

    final MutableVertex ferrari = db.newVertex("Supercar");
    ferrari.set("brand", "Ferrari");
    ferrari.set("type", "458 Italia");
    ferrari.set("year", 2014);
    ferrari.save();

    final MutableVertex luca = db.newVertex("Client");
    luca.set("firstName", "Luca");
    luca.set("lastName", "Skywalker");
    luca.save();

    luca.newEdge("Owns", maserati, true, "since", "2018");
    luca.newEdge("Owns", ducati, true, "since", "2016");
    luca.newEdge("Drives", ferrari, true, "since", "2018");

    db.commit();
  }

  @Test
  public void count() throws Exception {
    database.begin();
    try {
      // NON POLYMORPHIC COUNTING
      assertThat(database.countType("Vehicle", false)).isEqualTo(0);
      assertThat(database.countType("Car", false)).isEqualTo(1);
      assertThat(database.countType("Supercar", false)).isEqualTo(1);
      assertThat(database.countType("Motorcycle", false)).isEqualTo(1);

      assertThat(database.countType("Person", false)).isEqualTo(0);
      assertThat(database.countType("Client", false)).isEqualTo(1);

      assertThat(database.countType("Drives", false)).isEqualTo(1);
      assertThat(database.countType("Owns", false)).isEqualTo(2);

      // POLYMORPHIC COUNTING
      assertThat(database.countType("Vehicle", true)).isEqualTo(3);
      assertThat(database.countType("Car", true)).isEqualTo(2);
      assertThat(database.countType("Supercar", true)).isEqualTo(1);
      assertThat(database.countType("Motorcycle", true)).isEqualTo(1);

      assertThat(database.countType("Person", true)).isEqualTo(1);
      assertThat(database.countType("Client", true)).isEqualTo(1);

      assertThat(database.countType("Drives", true)).isEqualTo(3);
      assertThat(database.countType("Owns", true)).isEqualTo(2);

      assertThat((long) database.query("sql", "select count(*) as count from Vehicle").nextIfAvailable().getProperty("count")).isEqualTo(3L);

      assertThat((long) database.query("sql", "select count(*) as count from Vehicle WHERE $this INSTANCEOF Vehicle").nextIfAvailable()
        .getProperty("count")).isEqualTo(3L);

    } finally {
      database.commit();
    }
  }

  @Test
  public void scan() {
    database.begin();
    try {
      assertThat(scanAndCountType(database, "Vehicle", false)).isEqualTo(0);
      assertThat(scanAndCountType(database, "Car", false)).isEqualTo(1);
      assertThat(scanAndCountType(database, "Supercar", false)).isEqualTo(1);
      assertThat(scanAndCountType(database, "Motorcycle", false)).isEqualTo(1);

      assertThat(scanAndCountType(database, "Person", false)).isEqualTo(0);
      assertThat(scanAndCountType(database, "Client", false)).isEqualTo(1);

      assertThat(scanAndCountType(database, "Drives", false)).isEqualTo(1);
      assertThat(scanAndCountType(database, "Owns", false)).isEqualTo(2);

      // POLYMORPHIC COUNTING
      assertThat(scanAndCountType(database, "Vehicle", true)).isEqualTo(3);
      assertThat(scanAndCountType(database, "Car", true)).isEqualTo(2);
      assertThat(scanAndCountType(database, "Supercar", true)).isEqualTo(1);
      assertThat(scanAndCountType(database, "Motorcycle", true)).isEqualTo(1);

      assertThat(scanAndCountType(database, "Person", true)).isEqualTo(1);
      assertThat(scanAndCountType(database, "Client", true)).isEqualTo(1);

      assertThat(scanAndCountType(database, "Drives", true)).isEqualTo(3);
      assertThat(scanAndCountType(database, "Owns", true)).isEqualTo(2);

    } finally {
      database.commit();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1368
   */
  @Test
  public void testConstraintsInInheritance() {
    database.command("sql", "CREATE VERTEX TYPE V1");
    database.command("sql", "CREATE PROPERTY V1.prop1 STRING (mandatory true)");
    database.command("sql", "CREATE VERTEX TYPE V2 EXTENDS V1");
    try {
      database.command("sql",
          "INSERT INTO V1 SET prop2 = 'test'"); // this throws the exception as expected since I didn't set the mandatory prop1
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }

    try {
      database.command("sql",
          "INSERT INTO V2 SET prop2 = 'test'"); // this ignores the constraint on prop1 and insert the record although I didn't set the value
      fail("");
    } catch (ValidationException e) {
      // EXPECTED
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1438
   */
  @Test
  public void testBrokenInheritanceAfterTypeDropLast() {
    assertThat(database.countType("Vehicle", true)).isEqualTo(3);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Supercar");
      database.getSchema().dropType("Supercar");
    });
    assertThat(database.countType("Vehicle", true)).isEqualTo(2);
    assertThat(database.countType("Car", true)).isEqualTo(1);
    assertThat(database.countType("Motorcycle", true)).isEqualTo(1);
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1438
   */
  @Test
  public void testBrokenInheritanceAfterTypeDropMiddle() {
    assertThat(database.countType("Vehicle", true)).isEqualTo(3);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Car");
      database.getSchema().dropType("Car");
    });
    assertThat(database.countType("Vehicle", true)).isEqualTo(1);
    assertThat(database.countType("Motorcycle", true)).isEqualTo(1);
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1438
   */
  @Test
  public void testBrokenInheritanceAfterTypeDropFirst() {
    assertThat(database.countType("Vehicle", true)).isEqualTo(3);
    database.transaction(() -> {
      database.getSchema().dropType("Vehicle");
    });
    assertThat(database.countType("Car", true)).isEqualTo(2);
    assertThat(database.countType("Supercar", true)).isEqualTo(1);
    assertThat(database.countType("Motorcycle", true)).isEqualTo(1);
  }

  private int scanAndCountType(final Database db, final String type, final boolean polymorphic) {
    // NON POLYMORPHIC COUNTING
    final AtomicInteger counter = new AtomicInteger();
    db.scanType(type, polymorphic, record -> {
      assertThat(db.getSchema().getType(record.getTypeName()).instanceOf(type)).isTrue();
      counter.incrementAndGet();
      return true;
    });
    return counter.get();
  }
}
