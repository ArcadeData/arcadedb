/*
 * Copyright 2021 Arcade Data Ltd
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

package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class DDLTest extends TestHelper {
    @Override
    protected void beginTest() {

        database.transaction(db -> {
            db.command("sql", "CREATE VERTEX TYPE V");
            db.command("sql", "CREATE EDGE TYPE E");
        });

    }

    @Test
    void testGraphWithSql() {

        final int numOfElements = 10;
        //create schema: sript
        database.execute("sql",
                "BEGIN;" +
                        "CREATE VERTEX TYPE Person EXTENDS V; " +
                        "CREATE PROPERTY Person.name STRING;" +
                        "CREATE PROPERTY Person.id INTEGER;" +
                        "CREATE INDEX Person.id ON Person (id) UNIQUE NULL_STRATEGY SKIP;" +
                        "CREATE VERTEX TYPE Car EXTENDS V; " +
                        "CREATE PROPERTY Car.id INTEGER;" +
                        "CREATE PROPERTY Car.model STRING;" +
                        "CREATE INDEX Car.id ON Car (id) UNIQUE;" +
                        "CREATE EDGE TYPE Drives EXTENDS E;" +
                        "COMMIT;  " +
                        "");

        //vertices
        database.transaction(db -> {

            IntStream.range(0, numOfElements)
                    .forEach(i -> {
                        db.command("sql", "INSERT INTO Person set id=?,  name=?, surname=?", i, "Jay", "Miner" + i);
                        db.command("sql", "INSERT INTO Car set id=?,  brand=?, model=?", i, "Ferrari", "450" + i);
                    });


        });
        //edges
        database.transaction(db -> {

            IntStream.range(0, numOfElements)
                    .forEach(i -> {
                        db.command("sql", "CREATE EDGE Drives FROM (SELECT FROM Person WHERE id=?) TO (SELECT FROM Car WHERE id=?)", i, i);
                    });


        });

        database.transaction(db -> {

            db.query("sql", "SELECT FROM Drives")
                    .stream()
                    .map(r -> r.getEdge().get())
                    .peek(e -> assertThat(e.getIn()).isNotNull())
                    .peek(e -> assertThat(e.getOut()).isNotNull())
                    .forEach(e -> assertThat(e.getTypeName()).isEqualTo("Drives"));
        });

        database.transaction(db -> {

            final Long persons = db.command("sql", "SELECT count(*) as persons FROM Person ")
                    .next().<Long>getProperty("persons");

            Assertions.assertEquals(numOfElements, persons);

            final Long cars = db.command("sql", "SELECT count(*) as cars FROM Car")
                    .next().<Long>getProperty("cars");

            Assertions.assertEquals(numOfElements, cars);

            final Long vs = db.command("sql", "SELECT count(*) as vs FROM V")
                    .next().<Long>getProperty("vs");

            Assertions.assertEquals(numOfElements * 2, vs);

            final Long edges = db.command("sql", "SELECT count(*) as edges FROM Drives")
                    .next().<Long>getProperty("edges");

        });

    }


}
