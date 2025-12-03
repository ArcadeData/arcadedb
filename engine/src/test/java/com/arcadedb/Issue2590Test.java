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
package com.arcadedb;

import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for issue #2590 - Unique constraints not enforced during UPDATE operations.
 */
public class Issue2590Test extends TestHelper {

  public Issue2590Test() {
    autoStartTx = true;
  }

  @Test
  void databaseUpdateToNullThenToDuplicate() {
    // Create vertex type with unique field
    database.command("sql", "CREATE VERTEX TYPE NullRemoteTest");
    database.command("sql", "CREATE PROPERTY NullRemoteTest.licenseKey STRING");
    database.command("sql", "CREATE INDEX ON NullRemoteTest (licenseKey) UNIQUE NULL_STRATEGY SKIP");

    // Insert vertices
    database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-AAA', product = 'Product A'");
    database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-BBB', product = 'Product B'");
    database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-CCC', product = 'Product C'");

    // Update from LIC-BBB to null (should succeed)
    ResultSet updateToNull = database.command("sql", "UPDATE NullRemoteTest SET licenseKey = null WHERE licenseKey = 'LIC-BBB'");
    assertThat(updateToNull.hasNext()).isTrue();

    // Update from LIC-CCC to duplicate existing value LIC-AAA (should fail)
    assertThatThrownBy(() -> {
      database.command("sql", "UPDATE NullRemoteTest SET licenseKey = 'LIC-AAA' WHERE licenseKey = 'LIC-CCC'");
    }).isInstanceOf(DuplicatedKeyException.class)
        .as("UPDATE to duplicate value should enforce unique constraint");

    database.rollback();
  }

  @Test
  void uniqueConstraintOnUpdate() {
    // Create vertex type with unique field
    final VertexType vertexType = database.getSchema().createVertexType("TestVertex");
    vertexType.createProperty("uniqueField", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "TestVertex", "uniqueField");

    // Insert first vertex with value "A"
    final MutableVertex vertex1 = database.newVertex("TestVertex");
    vertex1.set("uniqueField", "A");
    vertex1.save();

    // Insert second vertex with value "B"
    final MutableVertex vertex2 = database.newVertex("TestVertex");
    vertex2.set("uniqueField", "B");
    vertex2.save();

    // Verify INSERT of duplicate value is correctly prevented
    assertThatThrownBy(() -> {
      final MutableVertex vertex3 = database.newVertex("TestVertex");
      vertex3.set("uniqueField", "A"); // Duplicate value
      vertex3.save();
    }).isInstanceOf(DuplicatedKeyException.class)
        .as("INSERT should have thrown DuplicatedKeyException");

    // UPDATE second vertex to use value "A" - should fail with DuplicatedKeyException
    // Reload vertex2 to ensure it's in a valid state
    final MutableVertex vertex2Reloaded = database.lookupByRID(vertex2.getIdentity(), true)
        .asVertex()
        .modify();
    assertThatThrownBy(() -> {
      vertex2Reloaded.set("uniqueField", "A");
      vertex2Reloaded.save();
    }).isInstanceOf(DuplicatedKeyException.class)
        .as("UPDATE should have thrown DuplicatedKeyException");

    database.rollback();
  }

  @Test
  void validUpdateToNonDuplicateValue() {
    // Create vertex type with unique field
    final VertexType vertexType = database.getSchema().createVertexType("TestVertex3");
    vertexType.createProperty("uniqueField", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "TestVertex3", "uniqueField");

    // Insert vertex
    final MutableVertex vertex1 = database.newVertex("TestVertex3");
    vertex1.set("uniqueField", "A");
    vertex1.save();

    // Update to a new unique value - should succeed
    vertex1.set("uniqueField", "C");
    vertex1.save();

    // Verify the update succeeded
    assertThat(vertex1.get("uniqueField")).isEqualTo("C");
  }
}
