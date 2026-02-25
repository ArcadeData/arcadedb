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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests MCP permission checking using semantic operation types.
 */
class MCPPermissionsTest {

  @Test
  void permissionCheckDeniesInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.CREATE), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void permissionCheckAllowsInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(true);

    // Should not throw
    ExecuteCommandTool.checkPermission(Set.of(OperationType.CREATE), config);
  }

  @Test
  void permissionCheckDeniesUpdate() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowUpdate(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.UPDATE), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void permissionCheckDeniesDelete() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowDelete(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.DELETE), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void permissionCheckDeniesSchemaChange() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowSchemaChange(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.SCHEMA), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void permissionCheckDeniesRead() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowReads(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.READ), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void upsertRequiresBothCreateAndUpdate() {
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
  void permissionCheckDeniesAdmin() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowAdmin(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(Set.of(OperationType.ADMIN), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void permissionCheckAllowsAdmin() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowAdmin(true);

    // Should not throw
    ExecuteCommandTool.checkPermission(Set.of(OperationType.ADMIN), config);
  }

  @Test
  void multipleOperationTypesAllChecked() {
    // A command that does both DELETE and UPDATE (like MOVE VERTEX)
    final Set<OperationType> moveOps = Set.of(OperationType.UPDATE, OperationType.DELETE);

    final MCPConfiguration configNoDelete = new MCPConfiguration("./target/test");
    configNoDelete.setAllowUpdate(true);
    configNoDelete.setAllowDelete(false);
    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(moveOps, configNoDelete))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void mcpDisabledReturnsError() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setEnabled(false);

    assertThat(config.isEnabled()).isFalse();
  }

  @Test
  void mcpUserAllowedCheck() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowedUsers(List.of("root", "admin"));

    assertThat(config.isUserAllowed("root")).isTrue();
    assertThat(config.isUserAllowed("admin")).isTrue();
    assertThat(config.isUserAllowed("unknown")).isFalse();
  }

  @Test
  void mcpWildcardUserAllowed() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowedUsers(List.of("*"));

    assertThat(config.isUserAllowed("anyuser")).isTrue();
    assertThat(config.isUserAllowed("root")).isTrue();
  }

  @Test
  void mcpNullUserNotAllowed() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowedUsers(List.of("root"));

    assertThat(config.isUserAllowed(null)).isFalse();
  }
}
