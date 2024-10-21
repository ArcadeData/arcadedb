/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFromGremlinTest {
  @Test
  public void testSQL()  {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testsql");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 50; i++)
          graph.getDatabase().newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      final ResultSet result = graph.sql("select name as `p.name`, age as `p.age` from Person where age >= :p1 ORDER BY `p.age`")//
          .setParameter(":p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        assertThat(row.<String>getProperty("p.name")).isEqualTo("Jay");
        assertThat(row.getProperty("p.age") instanceof Number).isTrue();
        assertThat((int) row.getProperty("p.age") > lastAge).isTrue();

        lastAge = row.getProperty("p.age");
      }

      assertThat(i).isEqualTo(25);

    } finally {
      graph.drop();
    }
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    FileUtils.deleteRecursively(new File("./target/testsql"));
  }
}
