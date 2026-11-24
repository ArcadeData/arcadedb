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
package com.arcadedb.remote;

import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12193: the index lookups of the remote client answered null or an empty list for an index that exists, and the index
 * creation methods returned null.
 */
class Issue12193RemoteIndexLookupIT extends BaseGraphServerTest {
  private RemoteDatabase database;

  @Override
  protected String getDatabaseName() {
    return "issue12193";
  }

  @BeforeEach
  void open() {
    database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  void closeDatabase() {
    if (database != null)
      database.close();
  }

  @Test
  void createReturnsTheIndexAndTheLookupsFindIt() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Customer12193").createProperty("email", Type.STRING);

    final TypeIndex created = schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Customer12193", "email");
    assertThat(created).isNotNull();
    assertThat(created.getName()).isEqualTo("Customer12193[email]");
    assertThat(created.getTypeName()).isEqualTo("Customer12193");
    assertThat(created.getPropertyNames()).containsExactly("email");
    assertThat(created.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(created.isUnique()).isTrue();
    assertThat(created.getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);

    final DocumentType type = schema.getType("Customer12193");
    assertThat(type.getIndexByProperties("email")).isEqualTo(created);
    assertThat(type.getIndexByProperties(List.of("email"))).isEqualTo(created);
    assertThat(type.getPolymorphicIndexByProperties("email")).isEqualTo(created);
    assertThat(type.getIndexesByProperties(List.of("email"))).containsExactly(created);
    assertThat(type.getIndexesByProperties("email")).containsExactly(created);
    assertThat(type.getAllIndexes(true)).containsExactly(created);

    // no index on another property: the "no index" answer stays null / empty
    type.createProperty("name", Type.STRING);
    assertThat(type.getIndexByProperties("name")).isNull();
    assertThat(type.getIndexesByProperties(List.of("name"))).isEmpty();

    // the idempotent create answers the existing index
    assertThat(type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "email")).isEqualTo(created);
  }

  @Test
  void subTypeFindsTheIndexOfItsSuperTypePolymorphically() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Parent12193").createProperty("code", Type.STRING);
    final TypeIndex parentIndex = schema.getType("Parent12193")
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
    assertThat(parentIndex).isNotNull();
    database.command("sql", "create document type Child12193 extends Parent12193");

    final DocumentType child = schema.getType("Child12193");
    assertThat(child.getIndexByProperties("code")).isNull();
    assertThat(child.getPolymorphicIndexByProperties("code")).isEqualTo(parentIndex);
    assertThat(child.getAllIndexes(false)).isEmpty();
    assertThat(child.getAllIndexes(true)).containsExactly(parentIndex);
  }

  @Test
  void theEntriesAreReadOnTheServerAndDropWorks() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Entry12193").createProperty("k", Type.STRING);
    final TypeIndex index = schema.getType("Entry12193").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");

    // never read by a client side planner, and a direct read says why instead of answering empty
    assertThat(index.isReadyForQueries()).isFalse();
    assertThatThrownBy(() -> index.get(new Object[] { "x" })).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::countEntries).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> index.setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR))
        .isInstanceOf(UnsupportedOperationException.class);

    index.drop();
    assertThat(schema.existsIndex("Entry12193[k]")).isFalse();
    assertThat(schema.getType("Entry12193").getIndexByProperties("k")).isNull();
  }
}
