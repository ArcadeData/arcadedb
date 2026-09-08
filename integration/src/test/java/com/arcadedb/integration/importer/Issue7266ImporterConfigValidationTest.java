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
 * Regression test for finding 1 of issue #7266.
 * <p>
 * Commit {@code 2a3cb60ef} taught the programmatic path to say what is wrong when an edge source declares no
 * endpoint - "Edge source 'X' declares no 'from' endpoint" - but the JSON path never reached it: it split the
 * {@code "attribute:VertexType"} value on {@code :} and read {@code [1]} unconditionally, so a value that forgot
 * the {@code :VertexType} half (precisely the mistake the new sentence describes) died on an
 * {@code ArrayIndexOutOfBoundsException} inside {@code Builder.edgeSource}'s eagerly-run consumer, before
 * {@code build()} could validate anything.
 * <p>
 * Each test therefore asserts two things that are NOT the same claim: that the exception is not the array one, and
 * that its message names the offending value. A config file is edited by hand, so the message is the whole fix -
 * an {@code IllegalArgumentException} saying nothing useful would pass a "does not throw AIOOBE" assertion while
 * leaving the operator exactly where they were.
 */
class Issue7266ImporterConfigValidationTest {

  private static final String DB_PATH = "target/databases/issue7266-importer-config-validation";

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

  @Test
  void aFromEndpointMissingItsVertexTypeIsReported() {
    assertThatThrownBy(() -> GraphImporter.fromJSON(database, edgeSourceConfig("PostId", "RelatedId:Post"), baseDir()))
        .as("the mistake the endpoint validation was written for must not die on an array index first")
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("LinkedTo")
        .hasMessageContaining("from")
        .hasMessageContaining("PostId");
  }

  @Test
  void aToEndpointMissingItsVertexTypeIsReported() {
    assertThatThrownBy(() -> GraphImporter.fromJSON(database, edgeSourceConfig("PostId:Post", "RelatedId"), baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("LinkedTo")
        .hasMessageContaining("to")
        .hasMessageContaining("RelatedId");
  }

  /**
   * The other two shapes the old code accepted in silence rather than crashing on: a third colon was dropped on the
   * floor, and an empty half became an empty attribute or vertex type that would match nothing, row after row.
   */
  @Test
  void anEndpointWithTheWrongNumberOfPartsIsReported() {
    for (final String malformed : new String[] { "PostId:Post:extra", ":Post", "PostId:", "", ":" })
      assertThatThrownBy(() -> GraphImporter.fromJSON(database, edgeSourceConfig(malformed, "RelatedId:Post"), baseDir()))
          .as("'%s' is not an 'attribute:VertexType' pair", malformed)
          .isInstanceOf(IllegalArgumentException.class)
          .isNotInstanceOf(ArrayIndexOutOfBoundsException.class)
          .hasMessageContaining("LinkedTo");
  }

  /**
   * An absent key is the same mistake one step earlier, and the one the programmatic path's sentence describes
   * word for word: "declares no 'from' endpoint". {@code JSONObject.getString(key)} threw a {@code JSONException}
   * naming the key and nothing else, so the reader still had to guess what the key was for.
   */
  @Test
  void anAbsentEndpointKeyIsReportedAsTheMissingEndpointItIs() {
    final JSONObject edgeSource = new JSONObject()
        .put("edge", "LinkedTo")
        .put("file", "issue7266-links.csv")
        .put("to", "RelatedId:Post");

    final JSONObject config = new JSONObject().put("edgeSources", new JSONArray().put(edgeSource));

    assertThatThrownBy(() -> GraphImporter.fromJSON(database, config, baseDir()))
        .as("an edge source with no 'from' key at all must read as a configuration mistake, not as a JSON one")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LinkedTo")
        .hasMessageContaining("from")
        .hasMessageContaining("attribute:VertexType");
  }

  /**
   * The public API has two {@code fromJSON} overloads. They funnel through the same parser, and this is the
   * assertion that says so rather than assuming it.
   */
  @Test
  void theStringOverloadReportsItToo() {
    final String json = edgeSourceConfig("PostId", "RelatedId:Post").toString();

    assertThatThrownBy(() -> GraphImporter.fromJSON(database, json, baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("PostId");
  }

  /**
   * The sibling of the same shape, in the same parser: a vertex source's {@code "filter"} is
   * {@code "attribute=value"} and was split on {@code =} with the same unguarded {@code [1]}.
   */
  @Test
  void aFilterWithoutAnEqualsIsReported() {
    final JSONObject vertex = new JSONObject()
        .put("type", "Post")
        .put("file", "issue7266-posts.csv")
        .put("id", "Id")
        .put("filter", "PostTypeId");

    final JSONObject config = new JSONObject().put("vertices", new JSONArray().put(vertex));

    assertThatThrownBy(() -> GraphImporter.fromJSON(database, config, baseDir()))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("Post")
        .hasMessageContaining("PostTypeId");
  }

  /**
   * The guard has to refuse only what is malformed. Without this the four tests above would all pass against a
   * parser that refused every config it was handed.
   */
  @Test
  void aWellFormedConfigStillParses() {
    final JSONObject vertex = new JSONObject()
        .put("type", "Post")
        .put("file", "issue7266-posts.csv")
        .put("id", "Id")
        .put("filter", "PostTypeId=1");

    final JSONObject config = edgeSourceConfig("PostId:Post", "RelatedId:Post")
        .put("vertices", new JSONArray().put(vertex));

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, baseDir())) {
      assertThat(importer).isNotNull();
    }
  }

  /** A config with one edge source, whose endpoints are whatever the caller passes. */
  private static JSONObject edgeSourceConfig(final String from, final String to) {
    final JSONObject edgeSource = new JSONObject()
        .put("edge", "LinkedTo")
        .put("file", "issue7266-links.csv")
        .put("from", from)
        .put("to", to);

    return new JSONObject().put("edgeSources", new JSONArray().put(edgeSource));
  }

  /**
   * The data files are never opened: {@code CsvRowSource} resolves its path lazily and reads it only from
   * {@code forEach}, which the config parse never calls. The directory only has to exist for the path join.
   */
  private static String baseDir() {
    return new File("target").getAbsolutePath();
  }
}
