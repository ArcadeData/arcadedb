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
package com.arcadedb.server;

/**
 * Raised when a database is created under a name that is already taken, either registered on the server or present as a
 * directory on disk (issue #10074). A conflict on the identity, not a malformed request: HTTP answers it 409 and gRPC
 * {@code ALREADY_EXISTS}.
 * <p>
 * It extends {@link IllegalArgumentException} because that is what the server raised before it had its own type, so an
 * embedded caller that catches the old type keeps working. The restore routes still raise a plain
 * {@link IllegalArgumentException} for a taken target and answer 400 / {@code INVALID_ARGUMENT}, which issue #7395 pins.
 */
public class DatabaseAlreadyExistsException extends IllegalArgumentException {
  public DatabaseAlreadyExistsException(final String message) {
    super(message);
  }
}
