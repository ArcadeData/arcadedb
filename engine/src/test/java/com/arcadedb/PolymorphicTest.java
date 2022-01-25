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
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PolymorphicTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      //------------
      // VEHICLES VERTICES
      //------------
      VertexType vehicle = database.getSchema().createVertexType("Vehicle", 3);
      vehicle.createProperty("brand", String.class);

      VertexType motorcycle = database.getSchema().createVertexType("Motorcycle", 3);
      motorcycle.addSuperType("Vehicle");

      try {
        motorcycle.createProperty("brand", String.class);
        Assertions.fail("Expected to fail by creating the same property name as the parent type");
      } catch (SchemaException e) {
      }

      Assertions.assertTrue(database.getSchema().getType("Motorcycle").instanceOf("Vehicle"));
      database.getSchema().createVertexType("Car", 3).addSuperType("Vehicle");
      Assertions.assertTrue(database.getSchema().getType("Car").instanceOf("Vehicle"));

      database.getSchema().createVertexType("Supercar", 3).addSuperType("Car");
      Assertions.assertTrue(database.getSchema().getType("Supercar").instanceOf("Car"));
      Assertions.assertTrue(database.getSchema().getType("Supercar").instanceOf("Vehicle"));

      //------------
      // PEOPLE VERTICES
      //------------
      VertexType person = database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Client").addSuperType(person);
      Assertions.assertTrue(database.getSchema().getType("Client").instanceOf("Person"));
      Assertions.assertFalse(database.getSchema().getType("Client").instanceOf("Vehicle"));

      //------------
      // EDGES
      //------------
      database.getSchema().createEdgeType("Drives");
      database.getSchema().createEdgeType("Owns").addSuperType("Drives");

      Assertions.assertTrue(database.getSchema().getType("Owns").instanceOf("Drives"));
      Assertions.assertFalse(database.getSchema().getType("Owns").instanceOf("Vehicle"));
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
      Assertions.assertEquals(0, database.countType("Vehicle", false));
      Assertions.assertEquals(1, database.countType("Car", false));
      Assertions.assertEquals(1, database.countType("Supercar", false));
      Assertions.assertEquals(1, database.countType("Motorcycle", false));

      Assertions.assertEquals(0, database.countType("Person", false));
      Assertions.assertEquals(1, database.countType("Client", false));

      Assertions.assertEquals(1, database.countType("Drives", false));
      Assertions.assertEquals(2, database.countType("Owns", false));

      // POLYMORPHIC COUNTING
      Assertions.assertEquals(3, database.countType("Vehicle", true));
      Assertions.assertEquals(2, database.countType("Car", true));
      Assertions.assertEquals(1, database.countType("Supercar", true));
      Assertions.assertEquals(1, database.countType("Motorcycle", true));

      Assertions.assertEquals(1, database.countType("Person", true));
      Assertions.assertEquals(1, database.countType("Client", true));

      Assertions.assertEquals(3, database.countType("Drives", true));
      Assertions.assertEquals(2, database.countType("Owns", true));

    } finally {
      database.commit();
    }
  }

  @Test
  public void scan() throws Exception {
    database.begin();
    try {
      Assertions.assertEquals(0, scanAndCountType(database, "Vehicle", false));
      Assertions.assertEquals(1, scanAndCountType(database, "Car", false));
      Assertions.assertEquals(1, scanAndCountType(database, "Supercar", false));
      Assertions.assertEquals(1, scanAndCountType(database, "Motorcycle", false));

      Assertions.assertEquals(0, scanAndCountType(database, "Person", false));
      Assertions.assertEquals(1, scanAndCountType(database, "Client", false));

      Assertions.assertEquals(1, scanAndCountType(database, "Drives", false));
      Assertions.assertEquals(2, scanAndCountType(database, "Owns", false));

      // POLYMORPHIC COUNTING
      Assertions.assertEquals(3, scanAndCountType(database, "Vehicle", true));
      Assertions.assertEquals(2, scanAndCountType(database, "Car", true));
      Assertions.assertEquals(1, scanAndCountType(database, "Supercar", true));
      Assertions.assertEquals(1, scanAndCountType(database, "Motorcycle", true));

      Assertions.assertEquals(1, scanAndCountType(database, "Person", true));
      Assertions.assertEquals(1, scanAndCountType(database, "Client", true));

      Assertions.assertEquals(3, scanAndCountType(database, "Drives", true));
      Assertions.assertEquals(2, scanAndCountType(database, "Owns", true));

    } finally {
      database.commit();
    }
  }

  private int scanAndCountType(final Database db, final String type, final boolean polymorphic) {
    // NON POLYMORPHIC COUNTING
    final AtomicInteger counter = new AtomicInteger();
    db.scanType(type, polymorphic, record -> {
      Assertions.assertTrue(db.getSchema().getType(record.getTypeName()).instanceOf(type));
      counter.incrementAndGet();
      return true;
    });
    return counter.get();
  }
}
