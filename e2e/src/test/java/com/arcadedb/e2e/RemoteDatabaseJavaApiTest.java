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
package com.arcadedb.e2e;

import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseJavaApiTest extends ArcadeContainerTemplate {
  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    database = new RemoteDatabase(host, httpPort, "beer", "root", "playwithdata");
    // ENLARGE THE TIMEOUT TO PASS THESE TESTS ON CI (GITHUB ACTIONS)
    database.setTimeout(60_000);
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
  }

  @Test
  void createUnidirectionalEdge() {

    database.command("sql", "create vertex type Person");
    database.command("sql", "create edge type FriendOf");

    MutableVertex me = database.newVertex("Person").set("name", "me").save();
    MutableVertex you = database.newVertex("Person").set("name", "you").save();

    MutableEdge friendOf = me.newEdge("FriendOf", you, false).save();

    assertThat(friendOf.getOut()).isEqualTo(me);
    assertThat(friendOf.getIn()).isEqualTo(you);

    me.getEdges(Vertex.DIRECTION.OUT, "FriendOf").forEach(e -> {
      assertThat(e).isEqualTo(friendOf);
    });

    database.query("sql", "select expand(out('FriendOf')) from Person where name = 'me'").stream().forEach(r -> {
      assertThat(r.<String>getProperty("name")).isEqualTo("you");
    });
  }

  @Test
  void createTypesAndDataWithSqlScripts() {
    //this is a test-double of HTTPGraphIT.testOneEdgePerTx test
    database.command("sqlscript",
        """
            CREATE VERTEX TYPE Photos;
            CREATE VERTEX TYPE Users;
            CREATE EDGE TYPE HasUploaded;""");

    database.command("sql", "CREATE VERTEX Users SET id = 'u1111'");

    database.command("sqlscript",
        """
            BEGIN;
            LET photo = CREATE VERTEX Photos SET id = "p12345", name = "download1.jpg";
            LET user = SELECT FROM Users WHERE id = "u1111";
            LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET kind = "User_Photos";
            COMMIT RETRY 30;
            RETURN $photo;""");

    database.command("sqlscript",
        """
            BEGIN;
            LET photo = CREATE VERTEX Photos SET id = "p2222", name = "download2.jpg";
            LET user = SELECT FROM Users WHERE id = "u1111";
            LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET kind = "User_Photos";
            COMMIT RETRY 30;
            RETURN $photo;""");

    database.command("sqlscript",
        """
            BEGIN;LET photo = CREATE VERTEX Photos SET id = "p5555", name = "download3.jpg";
            LET user = SELECT FROM Users WHERE id = "u1111";
            LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET kind = "User_Photos";
            COMMIT RETRY 30;
            RETURN $photo;""");

    ResultSet resultSet = database.command("sql",
        """
            SELECT expand( out('HasUploaded') ) FROM Users WHERE id = "u1111"
            """);

    assertThat(resultSet.stream()).hasSize(3);
  }
}
