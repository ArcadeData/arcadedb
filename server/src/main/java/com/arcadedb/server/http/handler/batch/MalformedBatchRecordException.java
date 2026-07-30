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
package com.arcadedb.server.http.handler.batch;

/**
 * The bytes read for a batch record did not form a complete, well-formed record.
 * <p>
 * This is deliberately narrower than "the payload is invalid": it marks the errors a <b>cut upload</b> can
 * fabricate. The last line of a truncated body arrives without its closing brace, or with fewer CSV fields than the
 * header declares, and the parser cannot tell that from a file the client really did generate that way - so before
 * such a record is blamed on the client, the handler checks whether the announced body actually arrived
 * ({@code PostBatchHandler.bodyEndedEarly}), and reports a truncated upload instead of pointing the user at a line
 * that is perfectly valid in their file (issue #5470).
 * <p>
 * The distinction to keep when adding a throw site: did the <b>bytes</b> fail to form a record (this exception), or
 * did a well-formed record carry the <b>wrong content</b> (plain {@link IllegalArgumentException})? A record naming a
 * temporary id that no vertex declared, an unparseable RID, a vertex after the first edge - those parsed fine, so no
 * number of further bytes can change the verdict and the client is answered at once. Only the first kind is
 * ambiguous, and only the first kind is worth waiting to be sure about.
 * <p>
 * Both map to HTTP 400 for a payload that really is malformed: this stays an {@link IllegalArgumentException} so the
 * status mapping in {@code PostBatchHandler} is unchanged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MalformedBatchRecordException extends IllegalArgumentException {

  public MalformedBatchRecordException(final String message) {
    super(message);
  }

  public MalformedBatchRecordException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
