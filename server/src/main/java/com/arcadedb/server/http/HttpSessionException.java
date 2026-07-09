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

import com.arcadedb.exception.TransactionException;

/**
 * Thrown when a request references an HTTP transaction session id that is no longer resolvable: it was
 * committed/rolled back, expired by the idle sweep, is owned by a different principal, or was invalidated
 * because its owner was dropped. Mapped to HTTP 404 by the request handler so a stale session id surfaces as
 * an explicit client error rather than a silent implicit-transaction commit or a 500.
 */
public class HttpSessionException extends TransactionException {
  public HttpSessionException(final String message) {
    super(message);
  }
}
