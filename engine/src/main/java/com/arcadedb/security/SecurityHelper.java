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
package com.arcadedb.security;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;

import java.util.List;

/**
 * Resolves the per-type ACL check for a {@link DocumentType} regardless of whether it owns a bucket a
 * {@link SecurityDatabaseUser#requestAccessOnFile(int, SecurityDatabaseUser.ACCESS)} can be checked against
 * (a normal document/vertex/edge type) or not (a TimeSeries type, whose data lives in its own engine): every
 * bucket of one type carries the identical access array (it is resolved from the type name alone), so checking
 * the first bucket is equivalent to checking all of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SecurityHelper {

  private SecurityHelper() {
  }

  /**
   * Throws a {@link SecurityException} if the current user (if any) is not entitled to {@code access} on
   * {@code type}. A {@code null} type is a no-op, consistent with the query executor steps that resolve a type
   * from a name and treat "not found" as "nothing to check".
   */
  public static void checkAccessOnType(final DatabaseInternal database, final DocumentType type, final SecurityDatabaseUser.ACCESS access) {
    if (type == null)
      return;

    final List<Integer> bucketIds = type.getBucketIds(false);
    if (!bucketIds.isEmpty())
      database.checkPermissionsOnFile(bucketIds.get(0), access);
    else
      database.checkPermissionsOnType(type.getName(), access);
  }

  /**
   * Same as {@link #checkAccessOnType(DatabaseInternal, DocumentType, SecurityDatabaseUser.ACCESS)}, resolving the
   * type that owns {@code index} first. A manual index (bound to no type) is a no-op: there is no type to gate.
   */
  public static void checkAccessOnIndex(final DatabaseInternal database, final Index index, final SecurityDatabaseUser.ACCESS access) {
    final String typeName = index.getTypeName();
    if (typeName == null)
      return;

    checkAccessOnType(database, database.getSchema().getType(typeName), access);
  }

  /**
   * Same as {@link #canAccessType(SecurityDatabaseUser, DocumentType, SecurityDatabaseUser.ACCESS)}, resolving the
   * user bound to {@code database}'s current context. For listings that must silently hide what the caller cannot
   * see rather than fail the whole request.
   */
  public static boolean canAccessType(final DatabaseInternal database, final DocumentType type, final SecurityDatabaseUser.ACCESS access) {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    return canAccessType(dbContext == null ? null : dbContext.getCurrentUser(), type, access);
  }

  /**
   * Non-throwing form for catalog listings (e.g. {@code SELECT FROM schema:types}, {@code schema:indexes}): whether
   * {@code type} should be visible to {@code user} at all, i.e. it grants {@code access} on at least one of the
   * type's buckets (or, for a bucket-less type, by name). A {@code null} user (no security context: embedded usage
   * or root) or a {@code null} type sees everything.
   */
  public static boolean canAccessType(final SecurityDatabaseUser user, final DocumentType type, final SecurityDatabaseUser.ACCESS access) {
    if (user == null || type == null)
      return true;

    final List<Integer> bucketIds = type.getBucketIds(false);
    if (bucketIds.isEmpty())
      return user.requestAccessOnType(type.getName(), access);

    for (final int bucketId : bucketIds)
      if (user.requestAccessOnFile(bucketId, access))
        return true;
    return false;
  }
}
