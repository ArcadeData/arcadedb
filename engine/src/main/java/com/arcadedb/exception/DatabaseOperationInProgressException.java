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
package com.arcadedb.exception;

/**
 * Raised when a whole-database maintenance operation - a backup, a restore or an import - is refused because
 * another one of them is already running on the same database, and the two cannot overlap. The per-database slot
 * that decides it is {@link com.arcadedb.engine.MaintenanceCoordinator}.
 * <p>
 * The distinction this type carries is "well formed, authorized, and retryable once the other operation finishes",
 * which is what lets a transport answer it with a conflict status instead of an internal error: HTTP maps it to
 * 409 and gRPC to {@code ABORTED}.
 * <p>
 * It lives in the engine rather than in the server because both sides raise it. The server's own entry points
 * ({@code trigger backup}, {@code restore database}, {@code restore backup}, {@code import database}) raise
 * {@code ServerControlPlane.OperationInProgressException}, which extends this, and the SQL statements
 * {@code BACKUP DATABASE} and {@code IMPORT DATABASE} - executed by the engine, which cannot see the server -
 * raise this one directly (issue #7443).
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class DatabaseOperationInProgressException extends ArcadeDBException {
  public DatabaseOperationInProgressException(final String message) {
    super(message);
  }
}
