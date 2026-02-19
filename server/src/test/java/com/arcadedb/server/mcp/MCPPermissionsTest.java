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
package com.arcadedb.server.mcp;

import com.arcadedb.query.OperationType;
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests MCP permission checking using semantic operation types.
 */
class MCPPermissionsTest {

  @Test
  void testPermissionCheckDeniesInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.CREATE), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void testPermissionCheckAllowsInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(true);

    // Should not throw
    ExecuteCommandTool.checkPermission(Set.of(OperationType.CREATE), config);
  }

  @Test
  void testPermissionCheckDeniesUpdate() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowUpdate(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.UPDATE), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void testPermissionCheckDeniesDelete() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowDelete(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.DELETE), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void testPermissionCheckDeniesSchemaChange() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowSchemaChange(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.SCHEMA), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void testPermissionCheckDeniesRead() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowReads(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.READ), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void testUpsertRequiresBothCreateAndUpdate() {
    // UPSERT produces both CREATE and UPDATE operations
    final Set<OperationType> upsertOps = Set.of(OperationType.CREATE, OperationType.UPDATE);

    // Should fail if insert is denied
    final MCPConfiguration configNoInsert = new MCPConfiguration("./target/test");
    configNoInsert.setAllowInsert(false);
    configNoInsert.setAllowUpdate(true);
    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(upsertOps, configNoInsert))
        .isInstanceOf(SecurityException.class);

    // Should fail if update is denied
    final MCPConfiguration configNoUpdate = new MCPConfiguration("./target/test");
    configNoUpdate.setAllowInsert(true);
    configNoUpdate.setAllowUpdate(false);
    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(upsertOps, configNoUpdate))
        .isInstanceOf(SecurityException.class);

    // Should pass if both are allowed
    final MCPConfiguration configBothAllowed = new MCPConfiguration("./target/test");
    configBothAllowed.setAllowInsert(true);
    configBothAllowed.setAllowUpdate(true);
    ExecuteCommandTool.checkPermission(upsertOps, configBothAllowed);
  }

  @Test
  void testPermissionCheckDeniesAdmin() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowAdmin(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.ADMIN), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void testPermissionCheckAllowsAdmin() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowAdmin(true);

    // Should not throw
    ExecuteCommandTool.checkPermission(Set.of(OperationType.ADMIN), config);
  }

  @Test
  void testMultipleOperationTypesAllChecked() {
    // A command that does both DELETE and UPDATE (like MOVE VERTEX)
    final Set<OperationType> moveOps = Set.of(OperationType.UPDATE, OperationType.DELETE);

    final MCPConfiguration configNoDelete = new MCPConfiguration("./target/test");
    configNoDelete.setAllowUpdate(true);
    configNoDelete.setAllowDelete(false);
    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(moveOps, configNoDelete))
        .isInstanceOf(SecurityException.class);
  }
}
