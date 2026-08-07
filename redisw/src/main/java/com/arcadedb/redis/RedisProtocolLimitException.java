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

import java.io.IOException;

/**
 * Thrown by {@code RedisNetworkExecutor.parseNext(int)} when an incoming RESP message violates a configured
 * protocol limit (array/bulk-string length, array nesting depth, or a malformed non-numeric length). It
 * extends {@link IOException}, not {@link RedisException}, because the violation is detected while decoding
 * the message itself, before {@code executeCommand} has a command to run: the parser has no reliable way to
 * know where the malformed structure ends, so the only safe response is to report the error and close the
 * connection, exactly as {@code RedisNetworkExecutor.run()} already does for other transport-level
 * {@link IOException}s.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisProtocolLimitException extends IOException {
  public RedisProtocolLimitException(final String message) {
    super(message);
  }
}
