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
 * A search needed the graph of a vector index built from scratch, and the heap gate judged that the build does not fit
 * the heap even with the smallest build cache it may run with and every evictable page of the read cache handed back
 * (issue #11497).
 * <p>
 * The search fails instead of answering. It cannot answer from the delta buffer alone, which holds only what was written
 * since the last graph and would be an incomplete result presented as a complete one, and it must not build anyway: on a
 * heap that is already that full, the build is what ends in an {@link OutOfMemoryError} that stops every database the
 * server hosts. Nothing of the build was started, so the same request re-issued once the heap has room (another build
 * finished, the load dropped, the JVM was given more heap) can succeed.
 * <p>
 * A {@link QueryHeapBudgetExceededException} for that reason: it is classified as retryable on every wire (503 on HTTP),
 * an {@code OPTIONAL CALL} keeps it rather than reading it as "no rows", and it is not a {@link NeedRetryException}, so a
 * commit loop does not retry it on the spot against the same full heap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorGraphBuildRefusedException extends QueryHeapBudgetExceededException {
  public VectorGraphBuildRefusedException(final String message) {
    super(message);
  }
}
