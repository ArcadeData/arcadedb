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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A refusal of {@link TypeIndexBuilder#checkUnderSchemaLock} on a replacing build must not cost the caller the index it would replace:
 * the check is asked before the drop, so a refusal leaves the old index in place.
 */
class TypeIndexBuilderRefusalUnderLockKeepsReplacedIndexTest extends TestHelper {

  @Test
  void aRefusalUnderTheLockKeepsTheIndexItWouldReplace() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("T");
    final String before = type.getIndexByProperties("p").getName();

    final TypeIndexBuilder builder = new TypeIndexBuilder((DatabaseInternal) database, "T", new String[] { "p" }) {
      @Override
      protected void checkUnderSchemaLock(final LocalDocumentType t, final TypeIndex replacing) {
        throw new SchemaException("refused under the lock");
      }
    };
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true);

    assertThatThrownBy(builder::create).isInstanceOf(SchemaException.class).hasMessageContaining("refused under the lock");
    assertThat(type.getIndexByProperties("p")).isNotNull();
    assertThat(type.getIndexByProperties("p").getName()).isEqualTo(before);
    assertThat(type.getIndexByProperties("p").isUnique()).isFalse();
  }
}
