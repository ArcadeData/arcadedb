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
package com.arcadedb.redis;

import com.arcadedb.exception.ArcadeDBException;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public class RedisException extends ArcadeDBException {
  private final String kind;

  public RedisException(final String message) {
    super(message);
    this.kind = null;
  }

  public RedisException(final String message, final Throwable cause) {
    super(message, cause);
    this.kind = null;
  }

  private RedisException(final String kind, final String message) {
    super(message);
    this.kind = kind;
  }

  /**
   * Creates a {@link RedisException} carrying an explicit RESP error kind - the token right after {@code -} in
   * the wire reply - separate from its message (issue #6560). Without this, a call site that needs a kind other
   * than the {@code ErrorCategory}-derived default (e.g. {@code WRONGPASS}, {@code NOAUTH}, {@code NOPROTO}) had
   * to bake it into the message text itself, which {@code RedisNetworkExecutor}'s dispatch {@code catch} block
   * then prefixed with its own {@code ErrorCategory}-derived kind (always {@code ERR} for a plain
   * {@code RedisException}) - producing a doubled/masking reply like {@code -ERR WRONGPASS ...} on the wire, where
   * the kind a client could actually branch on was always {@code ERR}.
   * <p>
   * Example: {@code RedisException.withKind("WRONGPASS", "invalid username-password pair")} replies
   * {@code -WRONGPASS invalid username-password pair}, not {@code -ERR WRONGPASS invalid username-password pair}.
   */
  public static RedisException withKind(final String kind, final String message) {
    return new RedisException(kind, message);
  }

  /**
   * The explicit RESP error kind this exception carries, or {@code null} when none was given - in which case
   * the kind should be derived from {@link com.arcadedb.exception.ErrorCategory} instead.
   */
  public String getKind() {
    return kind;
  }
}
