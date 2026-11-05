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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.DocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11286: the first commit of about 500 records failed with "Transaction buffer bigger than 2.00GB" when the bucket
 * page size was the minimum.
 */
class Issue11286MinimumPageSizeBatchCommitTest extends TestHelper {

  @Test
  void aPageSizeBelowTheMinimumIsRefused() {
    final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Tiny PAGESIZE " + (minimum - 1)).close())
        .hasMessageContaining(String.valueOf(minimum));
  }

  @Test
  void batchOf500RecordsCommitsAtTheMinimumBucketPageSize() {
    final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
    database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, minimum);

    final DocumentType type = database.getSchema().createDocumentType("Item");
    type.createProperty("id", Type.INTEGER);
    type.createProperty("name", Type.STRING);
    assertThat(((LocalBucket) type.getBuckets(false).getFirst()).getPageSize()).isEqualTo(minimum);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Item", "id");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "name");

    for (int b = 0; b < 2; b++) {
      final int base = b * 500;
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newDocument("Item").set("id", base + i).set("name", "name" + (base + i)).set("note", "n".repeat(1000)).save();
      });
    }
    assertThat(database.countType("Item", false)).isEqualTo(1000);
  }
}
