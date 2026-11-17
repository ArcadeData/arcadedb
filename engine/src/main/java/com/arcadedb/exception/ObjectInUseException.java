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
 * A statement that is well formed and names objects that exist, refused because of what the database holds now: a vertex or
 * edge type or bucket that still has records ({@code TRUNCATE TYPE}, {@code TRUNCATE BUCKET}, {@code DROP TYPE} without
 * {@code UNSAFE}), a property an index is built on ({@code DROP PROPERTY} without {@code FORCE}), a bucket a type owns
 * ({@code DROP BUCKET}), stored records that break a constraint the statement would declare ({@code CREATE PROPERTY ... (mandatory
 * true)}, {@code ALTER PROPERTY ... MAX}, issue #12436). The caller's to resolve - the
 * message says how - and never retryable: the same statement is refused again until the state changes.
 * <p>
 * A {@link CommandExecutionException}, which these refusals were raised as, so every caller that catches them keeps catching
 * them. As that plain type no rule classified them, and every protocol answered them as a server fault (HTTP 500, issue
 * #11803). Classified {@link ErrorCategory#VALIDATION}; HTTP answers it {@code 409 Conflict}, the state of the target resource
 * conflicting with the request (RFC 9110 15.5.10), and Postgres {@code 55006} object_in_use.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ObjectInUseException extends CommandExecutionException {
  public ObjectInUseException(final String message) {
    super(message);
  }

  public ObjectInUseException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
