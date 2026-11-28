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
 * A Cypher {@code DROP INDEX} that failed (issue #13686): no index holds the name, or the index belongs to a constraint. A
 * {@link SchemaException}, so every protocol answers it as it answered it before; what the subtype adds is the failure Neo4j
 * reports for it, {@code Neo.DatabaseError.Schema.IndexDropFailed}, which Bolt clients rely on to tell a failed drop from a
 * statement that means nothing against the schema.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class IndexDropFailedException extends SchemaException {
  public IndexDropFailedException(final String message) {
    super(message);
  }
}
