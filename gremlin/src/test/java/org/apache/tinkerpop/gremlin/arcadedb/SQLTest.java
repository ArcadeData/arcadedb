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
package org.apache.tinkerpop.gremlin.arcadedb;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLTest {
  @Test
  public void testSQL() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testsql");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 50; i++)
          graph.getDatabase().newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      ResultSet result = graph.sql("select name as `p.name`, age as `p.age` from Person where age >= :p1 ORDER BY `p.age`")//
          .setParameter(":p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        //System.out.println(row);

        Assertions.assertEquals("Jay", row.getProperty("p.name"));
        Assertions.assertTrue(row.getProperty("p.age") instanceof Number);
        Assertions.assertTrue((int) row.getProperty("p.age") > lastAge);

        lastAge = row.getProperty("p.age");
      }

      Assertions.assertEquals(25, i);

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
