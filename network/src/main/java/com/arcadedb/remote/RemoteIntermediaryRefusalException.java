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

import com.arcadedb.exception.NeedRetryException;

/**
 * A 503 the server did not write: its body is not JSON, so it comes from a hop in front of the server, an ingress, a load
 * balancer or a sidecar (issue #11387). Still a {@link NeedRetryException}, so a read, a replay-protected command and a
 * {@code begin} are retried as a 503 from the server is.
 * <p>
 * It does not carry the server's guarantee, though. The server answers 503 only for a request it refused before running
 * it, while a hop may answer 503 after it forwarded the request, when the upstream reset before its answer arrived. A
 * request that may write and cannot be replayed safely (a command without a request id, a {@code commit}) is therefore not
 * sent again on this exception: the hop may have forwarded it and the server applied it.
 */
public class RemoteIntermediaryRefusalException extends NeedRetryException {
  public RemoteIntermediaryRefusalException(final String message) {
    super(message);
  }
}
