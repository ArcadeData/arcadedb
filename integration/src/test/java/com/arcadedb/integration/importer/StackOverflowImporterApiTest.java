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
 * with Question/Answer split via row filter.
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
      db.getSchema().createEdgeType("ASKED");
      db.getSchema().createEdgeType("ANSWERED");
      db.getSchema().createEdgeType("TAGGED_WITH");
      db.getSchema().createEdgeType("HAS_ANSWER");
      db.getSchema().createEdgeType("ACCEPTED_ANSWER");
      db.getSchema().createEdgeType("COMMENTED_ON");
      db.getSchema().createEdgeType("COMMENTED_ON_ANSWER");
      db.getSchema().createEdgeType("WROTE_COMMENT");
      db.getSchema().createEdgeType("EARNED");
      db.getSchema().createEdgeType("LINKED_TO");
    });

    try (final GraphImporter importer = GraphImporter.builder(db)
        .limit(LIMIT)
        .vertex("Tag", XmlRowSource.from(DATA_DIR, "Tags.xml"), v -> {
          v.id("Id");
          v.idByName("TagName");
          v.intProperty("Id", "Id");
          v.property("TagName", "TagName");
          v.intProperty("Count", "Count");
        })
        .vertex("User", XmlRowSource.from(DATA_DIR, "Users.xml"), v -> {
          v.id("Id");
          v.intProperty("Id", "Id");
          v.property("DisplayName", "DisplayName");
          v.intProperty("Reputation", "Reputation");
          v.property("CreationDate", "CreationDate");
          v.intProperty("Views", "Views");
          v.intProperty("UpVotes", "UpVotes");
          v.intProperty("DownVotes", "DownVotes");
        })
        .vertex("Question", XmlRowSource.from(DATA_DIR, "Posts.xml"), v -> {
          v.filter("PostTypeId", "1");
          v.id("Id");
          v.intProperty("Id", "Id");
          v.property("Title", "Title");
          v.property("Body", "Body");
          v.intProperty("Score", "Score");
          v.intProperty("ViewCount", "ViewCount");
          v.property("CreationDate", "CreationDate");
          v.intProperty("AnswerCount", "AnswerCount");
          v.intProperty("CommentCount", "CommentCount");
          v.property("Tags", "Tags");
          v.edgeIn("OwnerUserId", "ASKED", "User");
          v.splitEdge("Tags", "TAGGED_WITH", "Tag", "|");
        })
        .vertex("Answer", XmlRowSource.from(DATA_DIR, "Posts.xml"), v -> {
          v.filter("PostTypeId", "2");
          v.id("Id");
          v.intProperty("Id", "Id");
          v.property("Body", "Body");
          v.intProperty("Score", "Score");
          v.property("CreationDate", "CreationDate");
          v.intProperty("CommentCount", "CommentCount");
          v.edgeIn("OwnerUserId", "ANSWERED", "User");
          v.edgeIn("ParentId", "HAS_ANSWER", "Question");
        })
        .vertex("Comment", XmlRowSource.from(DATA_DIR, "Comments.xml"), v -> {
          v.id("Id");
          v.intProperty("Id", "Id");
          v.intProperty("Score", "Score");
          v.property("CreationDate", "CreationDate");
          v.property("Text", "Text");
          v.edgeOut("PostId", "COMMENTED_ON", "Question");
          v.edgeOut("PostId", "COMMENTED_ON_ANSWER", "Answer");
          v.edgeIn("UserId", "WROTE_COMMENT", "User");
        })
        .vertex("Badge", XmlRowSource.from(DATA_DIR, "Badges.xml"), v -> {
          v.id("Id");
          v.intProperty("Id", "Id");
          v.property("Name", "Name");
          v.property("Date", "Date");
          v.intProperty("BadgeClass", "Class");
          v.boolProperty("TagBased", "TagBased");
          v.edgeIn("UserId", "EARNED", "User");
        })
        .edgeSource("ACCEPTED_ANSWER", XmlRowSource.from(DATA_DIR, "Posts.xml"), e -> {
          e.from("Id", "Question");
          e.to("AcceptedAnswerId", "Answer");
        })
        .edgeSource("LINKED_TO", XmlRowSource.from(DATA_DIR, "PostLinks.xml"), e -> {
          e.from("PostId", "Question");
          e.to("RelatedPostId", "Question");
          e.intProperty("LinkType", "LinkTypeId");
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
      assertThat(count("SELECT count(*) as c FROM User WHERE out('ANSWERED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE out('TAGGED_WITH').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE out('HAS_ANSWER').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE in('ASKED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Answer WHERE in('HAS_ANSWER').size() > 0")).isGreaterThan(0);
    });
  }

  @Test
  void hasAnswerConnectsAnswerToQuestion() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Question WHERE out('HAS_ANSWER').size() > 0 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        for (final var e : rs.next().getVertex().get().getEdges(Vertex.DIRECTION.OUT, "HAS_ANSWER"))
          assertThat(e.getInVertex().asVertex().getTypeName()).isEqualTo("Answer");
      }
    });
  }

  private long count(final String sql) {
    try (ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }
  }
}
