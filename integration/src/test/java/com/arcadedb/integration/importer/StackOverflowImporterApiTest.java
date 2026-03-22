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
 * Imports 10K rows per file from the StackOverflow dataset using the programmatic Java API.
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
      db.getSchema().createVertexType("Post");
      db.getSchema().createVertexType("Comment");
      db.getSchema().createVertexType("Badge");
      db.getSchema().createEdgeType("Posted");
      db.getSchema().createEdgeType("HasTag");
      db.getSchema().createEdgeType("AnswerOf");
      db.getSchema().createEdgeType("Accepted");
      db.getSchema().createEdgeType("Commented");
      db.getSchema().createEdgeType("CommentOn");
      db.getSchema().createEdgeType("Earned");
      db.getSchema().createEdgeType("LinkedTo");
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
        .vertex("Post", XmlRowSource.from(DATA_DIR, "Posts.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.intProperty("postType", "PostTypeId");
          v.property("title", "Title");
          v.property("body", "Body");
          v.intProperty("score", "Score");
          v.intProperty("viewCount", "ViewCount");
          v.property("creationDate", "CreationDate");
          v.intProperty("answerCount", "AnswerCount");
          v.intProperty("commentCount", "CommentCount");
          v.property("tags", "Tags");
          v.edgeIn("OwnerUserId", "Posted", "User");
          v.edgeOut("ParentId", "AnswerOf", "Post");
          v.edgeOut("AcceptedAnswerId", "Accepted", "Post");
          v.splitEdge("Tags", "HasTag", "Tag", "|");
        })
        .vertex("Comment", XmlRowSource.from(DATA_DIR, "Comments.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.intProperty("score", "Score");
          v.property("creationDate", "CreationDate");
          v.property("text", "Text");
          v.edgeOut("PostId", "CommentOn", "Post");
          v.edgeIn("UserId", "Commented", "User");
        })
        .vertex("Badge", XmlRowSource.from(DATA_DIR, "Badges.xml"), v -> {
          v.id("Id");
          v.intProperty("soId", "Id");
          v.property("name", "Name");
          v.property("date", "Date");
          v.intProperty("badgeClass", "Class");
          v.boolProperty("tagBased", "TagBased");
          v.edgeIn("UserId", "Earned", "User");
        })
        .edgeSource("LinkedTo", XmlRowSource.from(DATA_DIR, "PostLinks.xml"), e -> {
          e.from("PostId", "Post");
          e.to("RelatedPostId", "Post");
          e.intProperty("linkType", "LinkTypeId");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isGreaterThan(30_000);
      assertThat(importer.getEdgeCount()).isGreaterThan(10_000);
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
      assertThat(count("SELECT count(*) as c FROM Post")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Comment")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Badge")).isEqualTo(LIMIT);
    });
  }

  @Test
  void bidirectionalEdges() {
    database.transaction(() -> {
      assertThat(count("SELECT count(*) as c FROM User WHERE out('Posted').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Post WHERE out('HasTag').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Post WHERE out('AnswerOf').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Post WHERE in('Posted').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Tag WHERE in('HasTag').size() > 0")).isGreaterThan(0);
    });
  }

  @Test
  void answerOfConnectsAnswerToQuestion() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Post WHERE out('AnswerOf').size() > 0 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getVertex().get().getInteger("postType")).isEqualTo(2);
      }
    });
  }

  private long count(final String sql) {
    try (ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }
  }
}
