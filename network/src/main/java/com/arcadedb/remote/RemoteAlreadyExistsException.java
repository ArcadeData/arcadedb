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
package com.arcadedb.remote;

/**
 * Raised by the Java remote clients (HTTP and gRPC) when the server refuses a create because the name is already taken: a
 * user, an API token or a database (issue #10075). The server answers this as a conflict, HTTP 409 or gRPC
 * {@code ALREADY_EXISTS}, and it is neither a security refusal nor a duplicate key in an index, so it has a type of its
 * own that an application provisioning those objects can catch to treat "already there" as success.
 * <p>
 * It extends {@link RemoteException}, which is what the HTTP client raised for these conflicts before this type existed,
 * so a caller that caught that keeps working.
 */
public class RemoteAlreadyExistsException extends RemoteException {
  public RemoteAlreadyExistsException(final String message) {
    super(message);
  }

  public RemoteAlreadyExistsException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
