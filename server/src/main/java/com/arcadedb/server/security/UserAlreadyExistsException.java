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
package com.arcadedb.server.security;

import com.arcadedb.server.ServerException;

/**
 * Raised when a user is created under a name that is already taken (issue #10073). A conflict on the identity, not a
 * refusal by the security policy: it deliberately does NOT extend {@link ServerSecurityException}, because the HTTP
 * boundary answers that whole family 403 "Security error" - the status a non-root caller gets - and a client could then
 * not tell "this user already exists" from "you may not do this". HTTP answers it 409 and gRPC {@code ALREADY_EXISTS}.
 */
public class UserAlreadyExistsException extends ServerException {
  public UserAlreadyExistsException(final String userName) {
    super("User '" + userName + "' already exists");
  }
}
