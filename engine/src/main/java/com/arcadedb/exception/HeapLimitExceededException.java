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
package com.arcadedb.exception;

/**
 * An in-heap operation held more elements than the configured cap allows, or its query alone would hold more than the whole
 * {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_RAM} budget. A {@link CommandExecutionException}, so every caller that
 * failed the query on it still does; callers with a cheaper way out (a scan that can stream instead) tell it from the other
 * command failures by this type, and so does an {@code OPTIONAL CALL}, which must not read it as "no rows" (issue #10160).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HeapLimitExceededException extends CommandExecutionException {
  public HeapLimitExceededException(final String message) {
    super(message);
  }
}
