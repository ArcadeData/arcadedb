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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

public class MaxMinFromIndexStepTest {

  private static final String PROPERTY_NAME = "value";
  private static final String ALIAS_MAX     = "maxValue";
  private static final String ALIAS_MIN     = "minValue";

  @Test
  void shouldReturnMaxValueFromIndex() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert test data
      for (int i = 1; i <= 10; i++) {
        final MutableDocument document = db.newDocument(typeName);
        document.set(PROPERTY_NAME, i);
        document.save();
      }

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final MaxMinFromIndexStep step = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final ResultSet result = step.syncPull(context, 1);

      assertThat(result.hasNext()).isTrue();
      final Result maxResult = result.next();
      assertThat((Integer) maxResult.getProperty(ALIAS_MAX)).isEqualTo(10);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldReturnMinValueFromIndex() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert test data
      for (int i = 1; i <= 10; i++) {
        final MutableDocument document = db.newDocument(typeName);
        document.set(PROPERTY_NAME, i);
        document.save();
      }

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final MaxMinFromIndexStep step = new MaxMinFromIndexStep(index, false, PROPERTY_NAME, ALIAS_MIN, context);
      final ResultSet result = step.syncPull(context, 1);

      assertThat(result.hasNext()).isTrue();
      final Result minResult = result.next();
      assertThat((Integer) minResult.getProperty(ALIAS_MIN)).isEqualTo(1);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldReturnEmptyResultSetForEmptyIndex() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // No data inserted - index is empty

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final MaxMinFromIndexStep step = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final ResultSet result = step.syncPull(context, 1);

      // For empty indexes, the result set should be empty (no results)
      // This matches the behavior of the non-optimized aggregate path
      assertThat(result.hasNext()).as("Empty index should produce empty result set").isFalse();
    });
  }

  @Test
  void shouldHandleSingleValue() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert single value
      final MutableDocument document = db.newDocument(typeName);
      document.set(PROPERTY_NAME, 42);
      document.save();

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      // Test MAX
      final MaxMinFromIndexStep maxStep = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final ResultSet maxResult = maxStep.syncPull(context, 1);
      assertThat(maxResult.hasNext()).isTrue();
      assertThat((Integer) maxResult.next().getProperty(ALIAS_MAX)).isEqualTo(42);

      // Test MIN
      final MaxMinFromIndexStep minStep = new MaxMinFromIndexStep(index, false, PROPERTY_NAME, ALIAS_MIN, context);
      final ResultSet minResult = minStep.syncPull(context, 1);
      assertThat(minResult.hasNext()).isTrue();
      assertThat((Integer) minResult.next().getProperty(ALIAS_MIN)).isEqualTo(42);
    });
  }

  @Test
  void shouldHandleStringValues() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.STRING);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert string data
      final String[] values = { "apple", "banana", "cherry", "date", "elderberry" };
      for (final String value : values) {
        final MutableDocument document = db.newDocument(typeName);
        document.set(PROPERTY_NAME, value);
        document.save();
      }

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      // Test MAX (should return "elderberry" as it's lexicographically last)
      final MaxMinFromIndexStep maxStep = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final ResultSet maxResult = maxStep.syncPull(context, 1);
      assertThat(maxResult.hasNext()).isTrue();
      assertThat((String) maxResult.next().getProperty(ALIAS_MAX)).isEqualTo("elderberry");

      // Test MIN (should return "apple" as it's lexicographically first)
      final MaxMinFromIndexStep minStep = new MaxMinFromIndexStep(index, false, PROPERTY_NAME, ALIAS_MIN, context);
      final ResultSet minResult = minStep.syncPull(context, 1);
      assertThat(minResult.hasNext()).isTrue();
      assertThat((String) minResult.next().getProperty(ALIAS_MIN)).isEqualTo("apple");
    });
  }

  @Test
  void shouldHandleDuplicateValues() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert duplicate max and min values
      for (int i = 0; i < 5; i++) {
        final MutableDocument doc1 = db.newDocument(typeName);
        doc1.set(PROPERTY_NAME, 1);
        doc1.save();

        final MutableDocument doc2 = db.newDocument(typeName);
        doc2.set(PROPERTY_NAME, 100);
        doc2.save();
      }

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      // Test MAX
      final MaxMinFromIndexStep maxStep = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final ResultSet maxResult = maxStep.syncPull(context, 1);
      assertThat(maxResult.hasNext()).isTrue();
      assertThat((Integer) maxResult.next().getProperty(ALIAS_MAX)).isEqualTo(100);

      // Test MIN
      final MaxMinFromIndexStep minStep = new MaxMinFromIndexStep(index, false, PROPERTY_NAME, ALIAS_MIN, context);
      final ResultSet minResult = minStep.syncPull(context, 1);
      assertThat(minResult.hasNext()).isTrue();
      assertThat((Integer) minResult.next().getProperty(ALIAS_MIN)).isEqualTo(1);
    });
  }

  @Test
  void shouldResetAndExecuteAgain() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      // Insert test data
      for (int i = 1; i <= 10; i++) {
        final MutableDocument document = db.newDocument(typeName);
        document.set(PROPERTY_NAME, i);
        document.save();
      }

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final MaxMinFromIndexStep step = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);

      // First execution
      ResultSet result = step.syncPull(context, 1);
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty(ALIAS_MAX)).isEqualTo(10);
      assertThat(result.hasNext()).isFalse();

      // Reset
      step.reset();

      // Second execution should work
      result = step.syncPull(context, 1);
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty(ALIAS_MAX)).isEqualTo(10);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldProducePrettyPrintOutput() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final DocumentType type = TestHelper.createRandomType(db);
      type.createProperty(PROPERTY_NAME, Type.INTEGER);
      final String typeName = type.getName();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      final TypeIndex index = (TypeIndex) db.getSchema().getIndexByName(typeName + "[" + PROPERTY_NAME + "]");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final MaxMinFromIndexStep maxStep = new MaxMinFromIndexStep(index, true, PROPERTY_NAME, ALIAS_MAX, context);
      final String maxPrint = maxStep.prettyPrint(0, 2);
      assertThat(maxPrint).contains("MAX");
      assertThat(maxPrint).contains("INDEX");

      final MaxMinFromIndexStep minStep = new MaxMinFromIndexStep(index, false, PROPERTY_NAME, ALIAS_MIN, context);
      final String minPrint = minStep.prettyPrint(0, 2);
      assertThat(minPrint).contains("MIN");
      assertThat(minPrint).contains("INDEX");
    });
  }
}
