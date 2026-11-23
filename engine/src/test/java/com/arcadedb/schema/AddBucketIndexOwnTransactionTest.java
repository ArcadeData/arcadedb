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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code ALTER TYPE ... BUCKET +b} writes the header page of the bucket index it creates in a transaction of its own, not in the
 * transaction the statement runs in. The page rode the caller's transaction, so a caller that rolled back (or a leader that
 * published the caller's pages only when its commit came, after the schema entry that creates the file) left the file with no
 * header page: the index of the new bucket was "unavailable", the index of the type incomplete and, for a UNIQUE one, its
 * constraint not enforced on the bucket.
 */
class AddBucketIndexOwnTransactionTest extends TestHelper {
  private static final String TYPE = "AddBucketIndexItem";

  @Test
  void theIndexOfANewBucketDoesNotDependOnTheTransactionTheStatementRunsIn() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE, 1);
      type.createProperty("k", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "k");
      for (int i = 0; i < 10; i++)
        database.newDocument(TYPE).set("k", i).save();
    });
    database.getSchema().createBucket("addBucketIndexExtra");

    // The statement of a client: run inside a transaction, which a rollback then takes back
    database.begin();
    database.command("sql", "ALTER TYPE " + TYPE + " BUCKET +addBucketIndexExtra").close();
    database.rollback();

    reopenDatabase();

    final DocumentType type = database.getSchema().getType(TYPE);
    assertThat(type.getBuckets(false)).as("the bucket stays attached: a schema change is not undone by a rollback").hasSize(2);
    final TypeIndex index = type.getPolymorphicIndexByProperties("k");
    assertThat(index.isValid()).as("the index must hold a bucket index, with its header page, for every bucket").isTrue();

    // The constraint holds in the new bucket too
    database.transaction(() -> database.newDocument(TYPE).set("k", 100).save());
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO BUCKET:addBucketIndexExtra SET k = 100").close()))
        .isInstanceOf(DuplicatedKeyException.class);
  }
}
