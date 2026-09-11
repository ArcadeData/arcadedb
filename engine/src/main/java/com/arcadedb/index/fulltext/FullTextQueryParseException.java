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
package com.arcadedb.index.fulltext;

import com.arcadedb.index.IndexException;

/**
 * Raised when the text of a full-text query does not parse: an unbalanced quote, a dangling operator, a stray
 * {@code ~} or {@code ^}. It is the caller's mistake, and the search surfaces answer it as one (HTTP 400, gRPC
 * INVALID_ARGUMENT), which is why it has a type of its own: the plain {@link IndexException} it extends is also what
 * the index raises for an execution-time fault - a tokenizer or analyzer failure, a missing search engine - and
 * those must keep surfacing as internal errors with their stack trace rather than be misfiled as a bad request
 * (issue #7393). Extending {@link IndexException} keeps every caller that already catches the parent working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FullTextQueryParseException extends IndexException {
  public FullTextQueryParseException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
