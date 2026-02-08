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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.exception.CommandExecutionException;

/**
 * Marker object placed in results to indicate a deleted entity.
 * When a DELETE clause removes a node or relationship, the variable is replaced
 * with this marker. Subsequent property access or function calls (labels)
 * on the marker will throw DeletedEntityAccess errors per the OpenCypher spec.
 * type() is allowed on deleted relationships.
 */
public final class DeletedEntityMarker {
  public static final DeletedEntityMarker INSTANCE = new DeletedEntityMarker();

  private final String relationshipType;

  private DeletedEntityMarker() {
    this.relationshipType = null;
  }

  public DeletedEntityMarker(final String relationshipType) {
    this.relationshipType = relationshipType;
  }

  public String getRelationshipType() {
    return relationshipType;
  }

  public static void checkNotDeleted(final Object obj) {
    if (obj instanceof DeletedEntityMarker)
      throw new CommandExecutionException("DeletedEntityAccess: Cannot access a deleted entity");
  }

  @Override
  public String toString() {
    return "<deleted>";
  }
}
