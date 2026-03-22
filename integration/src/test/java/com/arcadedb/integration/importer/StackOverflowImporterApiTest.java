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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.XmlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Imports 10K rows per file from the StackOverflow dataset using the programmatic Java API
 * with Question/Answer split via row filter. Uses Neo4j edge naming convention.
 * Mirror of {@link StackOverflowImporterConfigTest} which uses JSON configuration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("datasetExists")
class StackOverflowImporterApiTest {

  private static final String DATA_DIR = "/Users/luca/Downloads/stackoverflow-large";
  private static final String DB_PATH  = "target/databases/stackoverflow-api-test";
  private static final long   LIMIT    = 10_000;

  private Database database;

  static boolean datasetExists() {
    return new File(DATA_DIR, "Posts.xml").exists();
  }

  @BeforeAll
  void importData() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Database db = new DatabaseFactory(DB_PATH).create();

    db.transaction(() -> {
      db.getSchema().createVertexType("Tag");
      db.getSchema().createVertexType("User");
      db.getSchema().createVertexType("Question");
      db.getSchema().createVertexType("Answer");
      db.getSchema().createVertexType("Comment");
      db.getSchema().createVertexType("Badge");
      db.getSchema().createEdgeType("POSTED");
      db.getSchema().createEdgeType("HAS_TAG");
      db.getSchema().createEdgeType("HAS_ANSWER");
      db.getSchema().createEdgeType("ACCEPTED");
      db.getSchema().createEdgeType("COMMENTED_ON");
      db.getSchema().createEdgeType("WROTE_COMMENT");
      db.getSchema().createEdgeType("EARNED");
      db.getSchema().createEdgeType("LINKED_TO");
    });

    try (final GraphImporter importer = GraphImporter.builder(db)
        .limit(LIMIT)
        .vertex("Tag", XmlRowSource.from(DATA_DIR, "Tags.xml"), v -> {
          v.id("Id");
          v.idByName("TagName");
          v.intProperty("soId", "Id");
          v.property("name", "TagName");
          v.intProperty("count", "Count");
        })
        .vertex("User", XmlRowSource.from(DATA_DIR, "Users.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.property("displayName", "DisplayName");
          v.intProperty("reputation", "Reputation");
          v.property("creationDate", "CreationDate");
          v.intProperty("views", "Views");
          v.intProperty("upVotes", "UpVotes");
          v.intProperty("downVotes", "DownVotes");
        })
        .vertex("Question", XmlRowSource.from(DATA_DIR, "Posts.xml"), v -> {
          v.filter("PostTypeId", "1");
          v.id("Id");
          v.intProperty("soId", "Id");
          v.property("title", "Title");
          v.property("body", "Body");
          v.intProperty("score", "Score");
          v.intProperty("viewCount", "ViewCount");
          v.property("creationDate", "CreationDate");
          v.intProperty("answerCount", "AnswerCount");
          v.intProperty("commentCount", "CommentCount");
          v.property("tags", "Tags");
          v.edgeIn("OwnerUserId", "POSTED", "User");
          v.edgeOut("AcceptedAnswerId", "ACCEPTED", "Answer");
          v.splitEdge("Tags", "HAS_TAG", "Tag", "|");
        })
        .vertex("Answer", XmlRowSource.from(DATA_DIR, "Posts.xml"), v -> {
          v.filter("PostTypeId", "2");
          v.id("Id");
          v.intProperty("soId", "Id");
          v.property("body", "Body");
          v.intProperty("score", "Score");
          v.property("creationDate", "CreationDate");
          v.intProperty("commentCount", "CommentCount");
          v.edgeIn("OwnerUserId", "POSTED", "User");
          v.edgeOut("ParentId", "HAS_ANSWER", "Question");
        })
        .vertex("Comment", XmlRowSource.from(DATA_DIR, "Comments.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.intProperty("score", "Score");
          v.property("creationDate", "CreationDate");
          v.property("text", "Text");
          v.edgeOut("PostId", "COMMENTED_ON", "Question");
          v.edgeOut("PostId", "COMMENTED_ON", "Answer");
          v.edgeIn("UserId", "WROTE_COMMENT", "User");
        })
        .vertex("Badge", XmlRowSource.from(DATA_DIR, "Badges.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.property("name", "Name");
          v.property("date", "Date");
          v.intProperty("badgeClass", "Class");
          v.boolProperty("tagBased", "TagBased");
          v.edgeIn("UserId", "EARNED", "User");
        })
        .edgeSource("LINKED_TO", XmlRowSource.from(DATA_DIR, "PostLinks.xml"), e -> {
          e.from("PostId", "Question");
          e.to("RelatedPostId", "Question");
          e.intProperty("linkType", "LinkTypeId");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isGreaterThan(30_000);
      assertThat(importer.getEdgeCount()).isGreaterThan(5_000);
    }

    database = db;
  }

  @AfterAll
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void vertexCounts() {
    database.transaction(() -> {
      assertThat(count("SELECT count(*) as c FROM Tag")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM User")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Question")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Answer")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Comment")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Badge")).isEqualTo(LIMIT);
    });
  }

  @Test
  void bidirectionalEdges() {
    database.transaction(() -> {
      assertThat(count("SELECT count(*) as c FROM User WHERE out('POSTED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE out('HAS_TAG').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Answer WHERE out('HAS_ANSWER').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE in('POSTED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE in('HAS_ANSWER').size() > 0")).isGreaterThan(0);
    });
  }

  @Test
  void hasAnswerConnectsAnswerToQuestion() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Answer WHERE out('HAS_ANSWER').size() > 0 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        for (final var e : rs.next().getVertex().get().getEdges(Vertex.DIRECTION.OUT, "HAS_ANSWER"))
          assertThat(e.getInVertex().asVertex().getTypeName()).isEqualTo("Question");
      }
    });
  }

  private long count(final String sql) {
    try (ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }
  }
}
