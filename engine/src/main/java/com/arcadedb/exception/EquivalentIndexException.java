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
 * An index was asked for that the type already has: an index on the same properties provides everything the request
 * asked for, under its own name or another one, and the request was not guarded by {@code IF NOT EXISTS} (issue #11392).
 * <p>
 * Not a name conflict, which {@link AlreadyExistsException} is: the name asked for may be free, and it is the DEFINITION
 * that exists already. What Neo4j calls an equivalent schema rule ({@code Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists}),
 * and what {@link ErrorCategory} classifies as {@link ErrorCategory#ALREADY_EXISTS}, so a wire protocol answers it as a
 * conflict on what exists rather than as a malformed argument.
 * <p>
 * An {@link IllegalArgumentException} because that is what the index builders raised for this refusal before, so an
 * embedded caller catching it keeps working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EquivalentIndexException extends IllegalArgumentException {
  public EquivalentIndexException(final String message) {
    super(message);
  }
}
