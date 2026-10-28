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
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MaxMinFromIndexStep}.
 */
class MaxMinFromIndexStepTest {

  private static final String ALIAS = "result";

  @Test
  void shouldReturnMaxValueFromIndex() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        db.newDocument("TestType").set("value", 10L).save();
        db.newDocument("TestType").set("value", 50L).save();
        db.newDocument("TestType").set("value", 30L).save();
        db.newDocument("TestType").set("value", 100L).save();
        db.newDocument("TestType").set("value", 25L).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final MaxMinFromIndexStep step = new MaxMinFromIndexStep((RangeIndex) index, ALIAS, true, null, context);

      final ResultSet result = step.syncPull(context, 1);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Long>getProperty(ALIAS)).isEqualTo(100L);
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Issue #10748: given the buckets of its target, the step reads the entries of those buckets only - through the sub-indexes of a
   * type index, and by skipping every other bucket's entry of any other range index.
   */
  @Test
  void shouldReadTheGivenBucketsOnly() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().buildDocumentType().withName("TestType").withTotalBuckets(2).create();
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");
      final int bucket0 = type.getBuckets(false).get(0).getFileId();
      final int bucket1 = type.getBuckets(false).get(1).getFileId();

      db.transaction(() -> {
        db.newDocument("TestType").set("value", 10L).save("TestType_0");
        db.newDocument("TestType").set("value", 20L).save("TestType_0");
        db.newDocument("TestType").set("value", 5L).save("TestType_1");
        db.newDocument("TestType").set("value", 900L).save("TestType_1");
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      assertThat(maxMin(index, true, new int[] { bucket0 }, context)).isEqualTo(20L);
      assertThat(maxMin(index, false, new int[] { bucket0 }, context)).isEqualTo(10L);
      assertThat(maxMin(index, true, new int[] { bucket0, bucket1 }, context)).isEqualTo(900L);
      assertThat(maxMin(index, false, new int[] { bucket1 }, context)).isEqualTo(5L);

      // a range index that is no type index: the entries of the other buckets are skipped one by one
      RangeIndex onBucket1 = null;
      for (final var subIndex : index.getIndexesOnBuckets())
        if (subIndex.getAssociatedBucketId() == bucket1)
          onBucket1 = (RangeIndex) subIndex;
      assertThat(onBucket1).isNotNull();
      assertThat(maxMin(onBucket1, true, new int[] { bucket1 }, context)).isEqualTo(900L);
      assertThat(maxMin(onBucket1, true, new int[] { bucket0 }, context)).isNull();
    });
  }

  private static Object maxMin(final RangeIndex index, final boolean max, final int[] buckets, final CommandContext context) {
    final ResultSet result = new MaxMinFromIndexStep(index, ALIAS, max, buckets, context).syncPull(context, 1);
    return result.next().getProperty(ALIAS);
  }

  @Test
  void shouldReturnMinValueFromIndex() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        db.newDocument("TestType").set("value", 10L).save();
        db.newDocument("TestType").set("value", 50L).save();
        db.newDocument("TestType").set("value", 30L).save();
        db.newDocument("TestType").set("value", 100L).save();
        db.newDocument("TestType").set("value", 5L).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final MaxMinFromIndexStep step = new MaxMinFromIndexStep((RangeIndex) index, ALIAS, false, null, context);

      final ResultSet result = step.syncPull(context, 1);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Long>getProperty(ALIAS)).isEqualTo(5L);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldReturnNullForEmptyIndex() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      // No data inserted

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final MaxMinFromIndexStep step = new MaxMinFromIndexStep((RangeIndex) index, ALIAS, true, null, context);

      final ResultSet result = step.syncPull(context, 1);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Long>getProperty(ALIAS)).isNull();
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldHaveCorrectPrettyPrintForMax() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final MaxMinFromIndexStep step = new MaxMinFromIndexStep((RangeIndex) index, ALIAS, true, null, context);

      final String prettyPrint = step.prettyPrint(0, 2);
      assertThat(prettyPrint).contains("MAX FROM INDEX");
      assertThat(prettyPrint).contains(index.getName());
    });
  }

  @Test
  void shouldHaveCorrectPrettyPrintForMin() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final MaxMinFromIndexStep step = new MaxMinFromIndexStep((RangeIndex) index, ALIAS, false, null, context);

      final String prettyPrint = step.prettyPrint(0, 2);
      assertThat(prettyPrint).contains("MIN FROM INDEX");
      assertThat(prettyPrint).contains(index.getName());
    });
  }
}
