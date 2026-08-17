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
 * Raised by a {@code WorkGuard} whose deadline came from a SQL {@code TIMEOUT n RETURN} clause: the statement asked
 * for the rows produced so far rather than for a failure, so the abort is a request to stop and yield, not an error.
 * <p>
 * It exists because a guard sitting inside a scan loop can only stop by throwing, while the clause it enforces
 * promises no exception. The step that owns the clause catches this one type and ends its result set there; every
 * other bound keeps throwing a plain {@link TimeoutException}. That is also why it must be its own type rather than a
 * flag on {@code TimeoutException}: catching the base type would swallow a genuine
 * {@code arcadedb.command.timeout} abort into a silently truncated answer.
 * <p>
 * A subclass of {@link TimeoutException} so that if one ever escapes the step - a shape of plan the pipeline does not
 * route through it - the caller still sees a timeout rather than an unrecognized failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PartialResultTimeoutException extends TimeoutException {
  public PartialResultTimeoutException(final String message) {
    super(message);
  }
}
