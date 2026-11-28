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
package com.arcadedb.index;

/**
 * A read of an index instance whose files a newer schema generation has retired, as on an HA replica that applied a compaction
 * while a query still held the instance (issue #12358). An {@link IndexException}, so a statement plans itself again on the
 * current generation; and a retryable failure on every wire when that is not enough (a burst of replicated compactions can
 * replace the index again under each new plan), never the bad request the {@link IllegalArgumentException} of the missing file
 * it carries as its cause would classify as.
 */
public class IndexReplacedException extends IndexException {
  public IndexReplacedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
