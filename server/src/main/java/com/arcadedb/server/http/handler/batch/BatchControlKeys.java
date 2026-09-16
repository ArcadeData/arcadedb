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

import java.util.Set;

/**
 * The five control keys of the {@code /api/v1/batch} encoding, and the rules that decide where each one may
 * appear.
 * <p>
 * Held in one place because the two encodings have to agree about the same payload model: JSONL reads the keys
 * off the object and CSV off the header row, but a client that models both line shapes with one struct - which
 * the gRPC sibling {@code GraphBatchRecord} invites, since it carries {@code temp_id} for both kinds - emits the
 * same misplacement in either encoding and must be told the same thing about it. Issue #7570 put the
 * "key I do not understand" refusal in both parsers by copying the message; issue #7574 adds the "key I
 * understand, on the line kind that cannot use it" refusal, and this class is what keeps the two from drifting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BatchControlKeys {
  static final String TYPE  = "@type";
  static final String CLASS = "@class";
  static final String ID    = "@id";
  static final String FROM  = "@from";
  static final String TO    = "@to";

  /** Every control key the encoding understands. Anything else beginning with {@code @} is refused (#7570). */
  static final Set<String> ALL = Set.of(TYPE, CLASS, ID, FROM, TO);

  /** Named in the refusal messages so the client is told what the five understood control keys are. */
  static final String LIST = TYPE + ", " + CLASS + ", " + ID + ", " + FROM + " and " + TO;

  private BatchControlKeys() {
  }

  /**
   * The refusal for a control key the encoding understands, sent on the kind of line that cannot use it: {@code @id}
   * on an edge, {@code @from} or {@code @to} on a vertex.
   * <p>
   * Before issue #7574 both were dropped in silence - read only on the other kind, then skipped as a control key when
   * the properties were collected - so the load answered 200 with the key neither stored nor reported. Refusing says
   * which key, on which line, and what to send instead; the alternative considered, counting the drops in the
   * response, only helps a client that reads the new counter.
   *
   * @param key        the control key that was sent
   * @param kind       the kind of line it was sent on
   * @param lineNumber 1-based line the key appeared on
   */
  static IllegalArgumentException misplaced(final String key, final BatchRecord.Kind kind, final int lineNumber) {
    return new IllegalArgumentException("Control key '" + key + "' at line " + lineNumber + " belongs to "
        + (kind == BatchRecord.Kind.VERTEX ? "an edge line, not to a vertex line" : "a vertex line, not to an edge line")
        + ": " + explain(key) + ". Remove it, or send the line as the other kind");
  }

  private static String explain(final String key) {
    if (ID.equals(key))
      return "a vertex declares '" + ID + "' so that edges in the same payload can reference it, while an edge is "
          + "identified by its '" + FROM + "' and '" + TO + "' endpoints and is never referenced by a temporary id";
    return "'" + FROM + "' and '" + TO + "' name an edge's endpoints; a vertex has none";
  }
}
