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
package com.arcadedb.mcp;

import com.arcadedb.security.SecurityDatabaseUser;

/**
 * The database principal an MCP tool runs as: the caller's own, narrowed by the MCP configuration of the database. The
 * operation analysis every write tool runs first can only judge the statement TEXT, and whether a statement changes the schema
 * is not always in its text: a Cypher {@code CREATE (:T)} or {@code MERGE (:T {...})} naming a type that does not exist creates
 * it, and analyzes as a CREATE. Binding this principal makes the engine refuse such a schema change at the one point every
 * route to it passes, {@code checkPermissionsOnDatabase(UPDATE_SCHEMA)}, which the engine demands only when the schema is about
 * to change (a type that exists as requested changes nothing): a configuration with {@code allowSchemaChange=false} then means
 * that no tool can leave the schema different from how it found it.
 * <p>
 * It can only narrow: every other decision is the caller's own principal's, so the per-type and per-database permissions of
 * the user still apply on top.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MCPScopedDatabaseUser implements SecurityDatabaseUser {
  private final SecurityDatabaseUser delegate;
  private final boolean              allowSchemaChange;

  private MCPScopedDatabaseUser(final SecurityDatabaseUser delegate, final boolean allowSchemaChange) {
    this.delegate = delegate;
    this.allowSchemaChange = allowSchemaChange;
  }

  /** The principal to bind for {@code permissions}: the caller's own when the configuration narrows nothing. */
  public static SecurityDatabaseUser of(final SecurityDatabaseUser user, final MCPPermissions permissions) {
    if (user == null || permissions == null || permissions.isAllowSchemaChange())
      return user;
    return new MCPScopedDatabaseUser(user, false);
  }

  @Override
  public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
    if (access == DATABASE_ACCESS.UPDATE_SCHEMA && !allowSchemaChange)
      return false;
    return delegate.requestAccessOnDatabase(access);
  }

  @Override
  public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
    return delegate.requestAccessOnFile(fileId, access);
  }

  @Override
  public boolean requestAccessOnType(final String typeName, final ACCESS access) {
    return delegate.requestAccessOnType(typeName, access);
  }

  @Override
  public boolean isServerAdministrator() {
    return delegate.isServerAdministrator();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public long getResultSetLimit() {
    return delegate.getResultSetLimit();
  }

  @Override
  public long getReadTimeout() {
    return delegate.getReadTimeout();
  }
}
