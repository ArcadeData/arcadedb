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

/**
 * The refresh of a {@link BackingTypeOwner} running on this thread: the one writer its backing type accepts (issue #11020). Opened
 * around the whole transaction of the refresh, its commit included, since an update is written to its page at commit.
 * <p>
 * Scopes nest: the refresh of a continuous aggregate commits rows of a type that can be the source of a materialized view, whose
 * refresh then runs on the same thread, inside the first scope. A refresh writes its own backing type and no other. The owner is
 * matched by its backing type, not by identity: a schema reload registers a new instance of the same view, and the refresh of the
 * instance it replaced must still be able to finish. Only a write to a backing type asks for the scope, so no other write ever
 * reads the thread local.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BackingTypeWriteScope implements AutoCloseable {
  private static final ThreadLocal<BackingTypeWriteScope> CURRENT = new ThreadLocal<>();

  private final BackingTypeOwner      owner;
  private final BackingTypeWriteScope outer;

  private BackingTypeWriteScope(final BackingTypeOwner owner, final BackingTypeWriteScope outer) {
    this.owner = owner;
    this.outer = outer;
  }

  static BackingTypeWriteScope open(final BackingTypeOwner owner) {
    final BackingTypeWriteScope scope = new BackingTypeWriteScope(owner, CURRENT.get());
    CURRENT.set(scope);
    return scope;
  }

  /** Whether the current thread runs a refresh of {@code owner}. */
  static boolean isRefreshing(final BackingTypeOwner owner) {
    for (BackingTypeWriteScope scope = CURRENT.get(); scope != null; scope = scope.outer)
      if (scope.owner == owner || scope.owner.getBackingTypeName().equals(owner.getBackingTypeName()))
        return true;
    return false;
  }

  @Override
  public void close() {
    if (outer == null)
      CURRENT.remove();
    else
      CURRENT.set(outer);
  }
}
