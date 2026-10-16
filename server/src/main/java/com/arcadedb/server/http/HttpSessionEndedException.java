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
 * Thrown by {@link HttpSession#execute} when the session a request resolved was ended while the request waited for its lock:
 * committed, rolled back, terminated or expired by another request. Answered 404 like any other unresolvable session, unless
 * the route knows a better answer, as a retried {@code /commit} of a session that committed does (issue #10171).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSessionEndedException extends HttpSessionException {
  private final String sessionId;

  public HttpSessionEndedException(final String sessionId) {
    super("Remote transaction '" + sessionId + "' not found or expired");
    this.sessionId = sessionId;
  }

  public String getSessionId() {
    return sessionId;
  }
}
