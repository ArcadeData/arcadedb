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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ResultInternal}.
 */
class ResultInternalTest {

  @Test
  void similarityProperty() {
    final ResultInternal result = new ResultInternal();
    result.setSimilarity(0.85f);

    assertThat(result.getSimilarity()).isEqualTo(0.85f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0.85f);
    assertThat(result.hasProperty("$similarity")).isTrue();
    assertThat(result.getPropertyNames()).contains("$similarity");
  }

  @Test
  void similarityDefaultsToZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.getSimilarity()).isEqualTo(0f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0f);
  }

  @Test
  void similarityNotInPropertyNamesWhenZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.hasProperty("$similarity")).isTrue(); // $similarity is always available
    assertThat(result.getPropertyNames()).doesNotContain("$similarity"); // but not in names when 0
  }

  @Test
  void similarityBoundaryValues() {
    final ResultInternal result = new ResultInternal();

    // Test minimum value
    result.setSimilarity(0.0f);
    assertThat(result.getSimilarity()).isEqualTo(0.0f);

    // Test maximum value
    result.setSimilarity(1.0f);
    assertThat(result.getSimilarity()).isEqualTo(1.0f);
    assertThat(result.getPropertyNames()).contains("$similarity");

    // Test intermediate value
    result.setSimilarity(0.5f);
    assertThat(result.getSimilarity()).isEqualTo(0.5f);
  }

  /**
   * Regression test for issue #4398: after removeProperty empties content of a specific key,
   * getProperty must not fall through to the backing element and resurface the original value.
   */
  @Test
  void getPropertyAfterRemoveDoesNotFallThroughToElement() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Person");
      final var doc = database.newDocument("Person");
      doc.set("name", "Alice");

      final ResultInternal result = new ResultInternal();
      result.setElement(doc);
      // Override in content layer
      result.setProperty("name", "Bob");
      assertThat(result.<String>getProperty("name")).isEqualTo("Bob");

      // After removal, the element value must NOT resurface
      result.removeProperty("name");
      assertThat(result.<String>getProperty("name")).isNull();
    });
  }

  /**
   * When content holds the key, its value shadows the element even when another key is also present.
   */
  @Test
  void getPropertyUsesContentWhenKeyPresent() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Item");
      final var doc = database.newDocument("Item");
      doc.set("color", "red");

      final ResultInternal result = new ResultInternal();
      result.setElement(doc);
      result.setProperty("color", "blue");
      result.setProperty("extra", "value"); // another key keeps content non-empty

      assertThat(result.<String>getProperty("color")).isEqualTo("blue");
    });
  }

  /**
   * A key absent from content still falls through to the backing element correctly.
   */
  @Test
  void getPropertyFallsThroughToElementWhenContentDoesNotContainKey() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Widget");
      final var doc = database.newDocument("Widget");
      doc.set("size", "large");

      final ResultInternal result = new ResultInternal();
      result.setElement(doc);
      // content is empty - no setProperty call

      assertThat(result.<String>getProperty("size")).isEqualTo("large");
    });
  }

  /**
   * Regression: with other keys present in content, accessing a removed key must return null,
   * not the element value (the original isEmpty-guard bug allowed fall-through here).
   */
  @Test
  void getPropertyDoesNotFallThroughWhenContentHasOtherKeys() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Node");
      final var doc = database.newDocument("Node");
      doc.set("label", "original");

      final ResultInternal result = new ResultInternal();
      result.setElement(doc);
      result.setProperty("label", "overridden");
      result.setProperty("unrelated", 42); // keeps content non-empty after removal

      result.removeProperty("label");

      // With the old isEmpty guard, "label" would return "original" because
      // content is still non-empty (contains "unrelated"). The fix uses containsKey.
      assertThat(result.<String>getProperty("label")).isNull();
    });
  }
}
