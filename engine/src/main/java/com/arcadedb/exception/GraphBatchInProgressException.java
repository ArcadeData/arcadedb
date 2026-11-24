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
 * Raised when a {@code GraphBatch} is refused because another one is already open on the same database: two overlapping
 * batches silently lose edges, so a database grants one at a time (issue #5666). Nothing of the refused batch ran, and the
 * same load succeeds once the other one is closed - a conflict with work in progress, not a fault. HTTP answers it 409, as it
 * does {@link DatabaseOperationInProgressException} (issue #11907).
 * <p>
 * A {@link DatabaseOperationException}, which is what the refusal was raised as before it had a type of its own, so a caller
 * that catches that keeps catching it.
 */
public class GraphBatchInProgressException extends DatabaseOperationException {
  public GraphBatchInProgressException(final String message) {
    super(message);
  }
}
