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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7302, item 3: the parse residuals #7266 left behind in the importer's JSON spec.
 * <p>
 * #7266 turned an {@code ArrayIndexOutOfBoundsException} into a sentence and the maintainer widened that
 * validation voluntarily. Three siblings of the widening survived, and all three are worse than a crash because
 * they are silent: a type prefix with nothing after it bound the property to an attribute named the empty string,
 * {@code parseDatetimeSpec} accepted a custom-format request with no format in it and read the separator as part
 * of the attribute name, and three mandatory keys still answered a bare {@code JSONException} that named the key
 * and nothing about what it is for.
 * <p>
 * Every assertion below therefore checks the message as well as the type, for the reason the #7266 suite gives:
 * a configuration file is edited by hand, so a message that says nothing useful leaves the operator exactly where
 * they were.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7302ImporterConfigResidualsTest {

  private static final String DB_PATH = "target/databases/issue7302-importer-config-residuals";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("LinkedTo");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /** A type prefix with no attribute after it reads nothing, row after row, and said so nowhere. */
  @Test
  void aTypePrefixWithNoAttributeIsReported() {
    for (final String spec : new String[] { "int:", "long:", "double:", "bool:", "vector:", "list:", "datetime:",
        "int: " })
      assertThatThrownBy(() -> GraphImporter.fromJSON(database, vertexWithProperty(spec), baseDir()))
          .as("'%s' names no source attribute", spec)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score")
          .hasMessageContaining(spec.trim());
  }

  /** And neither does a value that is empty outright. */
  @Test
  void anEmptyPropertySpecIsReported() {
    assertThatThrownBy(() -> GraphImporter.fromJSON(database, vertexWithProperty(""), baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("score");
  }

  /**
   * A pipe is a request for a custom format, and both halves of that request have to be there. The old test was
   * {@code pipe > 0}, so a missing format half took the no-format branch and bound the property to an attribute
   * literally named {@code "|attr"}.
   */
  @Test
  void aDatetimeSpecMissingHalfOfItsCustomFormatIsReported() {
    for (final String spec : new String[] { "datetime:|pickup_time", "datetime:yyyy-MM-dd|", "datetime:|" })
      assertThatThrownBy(() -> GraphImporter.fromJSON(database, vertexWithProperty(spec), baseDir()))
          .as("'%s' is half a FORMAT|ATTRIBUTE pair", spec)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score");
  }

  /** Both well-formed datetime spellings still parse, or the guard above would be refusing the feature. */
  @Test
  void bothWellFormedDatetimeSpecsStillParse() {
    for (final String spec : new String[] { "datetime:pickup_time", "datetime:yyyy-MM-dd'T'HH:mm:ss|pickup_time" })
      try (final GraphImporter importer = GraphImporter.fromJSON(database, vertexWithProperty(spec), baseDir())) {
        assertThat(importer).as("'%s' is a valid spec", spec).isNotNull();
      }
  }

  /**
   * The three absent keys. Each is the same mistake as an absent edge endpoint, one step earlier, and each used
   * to answer {@code JSONObject["type"] not found} - true, and no help to someone holding a file that has to say
   * something they were never told.
   */
  @Test
  void anAbsentVertexTypeIsReportedAsTheMissingKeyItIs() {
    final JSONObject vertex = new JSONObject().put("file", "issue7302-posts.csv").put("id", "Id");

    assertThatThrownBy(() -> GraphImporter.fromJSON(database, verticesConfig(vertex), baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("vertex source")
        .hasMessageContaining("type");
  }

  @Test
  void anAbsentEdgeTypeIsReportedAsTheMissingKeyItIs() {
    final JSONObject edgeSource = new JSONObject()
        .put("file", "issue7302-links.csv")
        .put("from", "PostId:Post")
        .put("to", "RelatedId:Post");

    assertThatThrownBy(() -> GraphImporter.fromJSON(database,
        new JSONObject().put("edgeSources", new JSONArray().put(edgeSource)), baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("edge source")
        .hasMessageContaining("edge");
  }

  @Test
  void anAbsentSourceFileIsReportedAgainstTheSourceThatDeclaresIt() {
    final JSONObject vertex = new JSONObject().put("type", "Post").put("id", "Id");

    assertThatThrownBy(() -> GraphImporter.fromJSON(database, verticesConfig(vertex), baseDir()))
        .as("the message names WHICH source has no file, since a config has many")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Post")
        .hasMessageContaining("file");
  }

  @Test
  void anEdgesEntryMissingOneOfItsThreeKeysIsReported() {
    for (final String absent : new String[] { "attribute", "edge", "target" }) {
      final JSONObject edge = new JSONObject()
          .put("attribute", "FriendId")
          .put("edge", "LinkedTo")
          .put("target", "Post");
      edge.remove(absent);

      final JSONObject vertex = new JSONObject()
          .put("type", "Post")
          .put("file", "issue7302-posts.csv")
          .put("id", "Id")
          .put("edges", new JSONArray().put(edge));

      assertThatThrownBy(() -> GraphImporter.fromJSON(database, verticesConfig(vertex), baseDir()))
          .as("an \"edges\" entry with no '%s'", absent)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Post")
          .hasMessageContaining(absent);
    }
  }

  /**
   * The command-line path reaches {@code createSchemaFromConfig} BEFORE {@code fromJSON}, so a config missing one
   * of these keys was answered there by the bare {@code JSONException} and never reached the sentence written for
   * it (PR #7314 review). Both entry points now read the same way.
   */
  @Test
  void theSchemaCreationPathReportsTheSameMissingKeys() {
    final JSONObject vertexWithNoType = new JSONObject().put("file", "issue7302-posts.csv");
    assertThatThrownBy(() -> GraphImporter.createSchemaFromConfig(database, verticesConfig(vertexWithNoType)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("vertex source")
        .hasMessageContaining("type");

    final JSONObject edgeWithNoType = new JSONObject().put("attribute", "FriendId").put("target", "Post");
    final JSONObject vertex = new JSONObject()
        .put("type", "Post")
        .put("file", "issue7302-posts.csv")
        .put("edges", new JSONArray().put(edgeWithNoType));
    assertThatThrownBy(() -> GraphImporter.createSchemaFromConfig(database, verticesConfig(vertex)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Post")
        .hasMessageContaining("edge");

    final JSONObject edgeSourceWithNoType = new JSONObject().put("file", "issue7302-links.csv");
    assertThatThrownBy(() -> GraphImporter.createSchemaFromConfig(database,
        new JSONObject().put("edgeSources", new JSONArray().put(edgeSourceWithNoType))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("edge source")
        .hasMessageContaining("edge");
  }

  /** And it still creates the types a well-formed config declares. */
  @Test
  void theSchemaCreationPathStillCreatesWhatItIsGiven() {
    final JSONObject vertex = new JSONObject()
        .put("type", "Comment")
        .put("file", "issue7302-posts.csv")
        .put("edges", new JSONArray().put(new JSONObject()
            .put("attribute", "PostId").put("edge", "CommentOn").put("target", "Post")));

    GraphImporter.createSchemaFromConfig(database, verticesConfig(vertex));

    assertThat(database.getSchema().existsType("Comment")).isTrue();
    assertThat(database.getSchema().existsType("CommentOn")).isTrue();
  }

  /**
   * The guard has to refuse only what is malformed. Without this every test above would pass against a parser
   * that refused every config it was handed.
   */
  @Test
  void aWellFormedConfigStillParses() {
    final JSONObject edge = new JSONObject()
        .put("attribute", "FriendId")
        .put("edge", "LinkedTo")
        .put("target", "Post");

    final JSONObject vertex = new JSONObject()
        .put("type", "Post")
        .put("file", "issue7302-posts.csv")
        .put("id", "Id")
        .put("properties", new JSONObject().put("score", "int:Score").put("name", "DisplayName"))
        .put("edges", new JSONArray().put(edge));

    try (final GraphImporter importer = GraphImporter.fromJSON(database, verticesConfig(vertex), baseDir())) {
      assertThat(importer).isNotNull();
    }
  }

  private static JSONObject vertexWithProperty(final String spec) {
    final JSONObject vertex = new JSONObject()
        .put("type", "Post")
        .put("file", "issue7302-posts.csv")
        .put("id", "Id")
        .put("properties", new JSONObject().put("score", spec));

    return verticesConfig(vertex);
  }

  private static JSONObject verticesConfig(final JSONObject vertex) {
    return new JSONObject().put("vertices", new JSONArray().put(vertex));
  }

  /**
   * The data files are never opened: {@code CsvRowSource} resolves its path lazily and reads it only from
   * {@code forEach}, which the config parse never calls.
   */
  private static String baseDir() {
    return new File("target").getAbsolutePath();
  }
}
