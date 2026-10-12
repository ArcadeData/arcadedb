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
package com.arcadedb.server.http;

/**
 * A request a handler refuses BEFORE a single statement of it reached the engine (issue #9784): the caller asked for something
 * the route cannot give - a streaming encoding for a statement that is not read-only, a serializer with no row stream - and the
 * answer is a 400.
 * <p>
 * It is an {@link IllegalArgumentException}, so every layer that maps one to a 400 keeps doing so. What it adds is the
 * statement that nothing ran: {@link HttpSession#execute} rolls the session's transaction back when a request fails because
 * "the command failed part way, so what it was running in cannot be trusted", and a request that never started has nothing
 * to distrust. Throw it ONLY where that is provable, that is ahead of the call that hands the statement to the engine; a
 * refusal raised once the statement has run (a result over the row ceiling, a streamed {@code EXPLAIN}) is not one, because a
 * script can have written before it got there.
 * <p>
 * That covers the validation of the request itself, not only the negotiation of its encoding (issue #9852): a missing or
 * mistyped field of the envelope, a {@code limit} the server cannot apply, a typed parameter marker that does not decode. The
 * helpers that validate those ({@code requireIntLimit}, {@code decodeTypedJsonMarkers}) throw this type themselves, so every
 * route that reads a field through them gets the rule without having to remember it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RequestRefusedBeforeExecutionException extends IllegalArgumentException {
  public RequestRefusedBeforeExecutionException(final String message) {
    super(message);
  }

  public RequestRefusedBeforeExecutionException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
