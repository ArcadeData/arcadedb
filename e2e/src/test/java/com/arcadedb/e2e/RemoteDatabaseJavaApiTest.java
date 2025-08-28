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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.IntStream;

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
  void backupDatabase() throws IOException, InterruptedException {

    ResultSet resultSet = database.command("sql", "BACKUP DATABASE");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).startsWith("beer-backup-");

    resultSet = database.command("sqlscript", "BACKUP DATABASE");

    result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).startsWith("beer-backup-");

    resultSet = database.command("sqlscript", """
        BEGIN;
        INSERT INTO Beer SET name = 'beer1';
        COMMIT;
        BACKUP DATABASE;
        """);

    result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).startsWith("beer-backup-");

  }

  @Test
  void createUnidirectionalEdge() {

    database.command("sql", "create vertex type Person");
    database.command("sql", "create edge type FriendOf");

    MutableVertex me = database.newVertex("Person").set("name", "me").save();
    MutableVertex you = database.newVertex("Person").set("name", "you").save();

    MutableEdge friendOf = me.newEdge("FriendOf", you).save();

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

  @Test
  void renameTypeAndAliases() {

    database.command("sql", "ALTER TYPE Beer NAME Birra");
    database.command("sql", "ALTER TYPE Birra ALIASES Beer");

    ResultSet result = database.query("sql", "select * from Beer limit 10");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);

    result = database.query("sql", "select * from Birra limit 10");
    assertThat(result.stream().count()).isEqualTo(10);

  }

  @Test
  @Disabled
  void testMultipleInsert() throws SQLException, ClassNotFoundException {
    database.command("sqlscript", """
        create vertex type `TEXT_EMBEDDING` if not exists;
        create property TEXT_EMBEDDING.str if not exists STRING;
        create property TEXT_EMBEDDING.embedding if not exists ARRAY_OF_FLOATS;
        """);

    LocalDateTime start = LocalDateTime.now();

    IntStream.range(1, 100001).forEach(i -> {

      database.command("sqlscript",
          "INSERT INTO `TEXT_EMBEDDING` SET str = meow_%d, embedding = [0.1,0.2,0.3] RETURN embedding;".formatted(i));

    });

    LocalDateTime end = LocalDateTime.now();
    System.out.println("Execution time: " + Duration.between(start, end).toSeconds() + " seconds");

    ResultSet resultSet = database.query("sql", "SELECT count() as count FROM `TEXT_EMBEDDING`");
    System.out.println("Count: " + resultSet.stream().findFirst().get().getProperty("count"));

  }

  @Test
  @Disabled
  void testMultipleInsertBAtched() throws SQLException, ClassNotFoundException {
    database.command("sqlscript", """
        create vertex type `TEXT_EMBEDDING` if not exists;
        create property TEXT_EMBEDDING.str if not exists STRING;
        create property TEXT_EMBEDDING.embedding if not exists ARRAY_OF_FLOATS;
        """);

    LocalDateTime start = LocalDateTime.now();
    StringBuilder sb = new StringBuilder();

    IntStream.range(1, 100001).forEach(i -> {

      sb.append("INSERT INTO `TEXT_EMBEDDING` SET str = meow_%d, embedding = [0.1,0.2,0.3] RETURN embedding;".formatted(i));
      sb.append("\n");
      if (i % 500 == 0) {
        System.out.printf("Inserting %d%n", i);
        database.transaction(() -> {

          String string = sb.toString();
          database.command("sqlscript", string);

        });
        sb.setLength(0);
      }

    });
    LocalDateTime end = LocalDateTime.now();
    System.out.println("Execution time: " + Duration.between(start, end).toSeconds() + " seconds");

    ResultSet resultSet = database.query("sql", "SELECT count() as count FROM `TEXT_EMBEDDING`");
    System.out.println("Count: " + resultSet.stream().findFirst().get().getProperty("count"));
  }

  @Test
  @Disabled
  void testMultipleInsertSingleTransaction() throws SQLException, ClassNotFoundException {
    database.command("sqlscript", """
        create vertex type `TEXT_EMBEDDING` if not exists;
        create property TEXT_EMBEDDING.str if not exists STRING;
        create property TEXT_EMBEDDING.embedding if not exists ARRAY_OF_FLOATS;
        """);

    LocalDateTime start = LocalDateTime.now();
    StringBuilder sb = new StringBuilder();

    database.transaction(() -> {
      IntStream.range(1, 100001).forEach(i -> {

        database.command("sql",
            "INSERT INTO `TEXT_EMBEDDING` SET str = meow_%d, embedding = [0.1,0.2,0.3];".formatted(i));

      });

    });
    LocalDateTime end = LocalDateTime.now();
    System.out.println("Execution time: " + Duration.between(start, end).toSeconds() + " seconds");

    ResultSet resultSet = database.query("sql", "SELECT count() as count FROM `TEXT_EMBEDDING`");
    System.out.println("Count: " + resultSet.stream().findFirst().get().getProperty("count"));

  }

}
