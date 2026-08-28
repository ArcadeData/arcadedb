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

import java.io.IOException;

/**
 * The bytes read out of a page do not make sense: a slot-table entry pointing inside the page header, a record
 * marker where none can be, an offset past the end of the content. The page was READ successfully - this says
 * nothing about the disk, and everything about what is on it.
 * <p>
 * It exists to separate the two opposite meanings {@link IOException} used to carry inside one chunk-chain walk
 * (issue #6282, item 3). {@code LocalBucket.findBrokenChunkChain} answers "cannot prove a break" for a page it could
 * not LOAD - an I/O fault is not evidence about a record - and "the chain is broken" for a page whose CONTENTS are
 * nonsense; before this type the only thing keeping the two apart was where the {@code try}/{@code catch} sat, a
 * positional invariant that the next restructuring of that walk had no type-level signal to preserve. The
 * consequence of getting it backwards is not academic: since #6258 a confirmed break is permanent and non-retryable,
 * and sends the operator to {@code CHECK DATABASE FIX}, which DELETES the record.
 * <p>
 * It extends {@link IOException} deliberately rather than being an unchecked {@link ArcadeDBException}: every page
 * accessor on this path already declares {@code throws IOException}, and every caller that today treats a corrupt
 * offset as an I/O failure keeps compiling and keeps behaving as it did. Only the callers that WANT the distinction
 * catch this type first - which, because a subclass must be caught before its supertype, the compiler enforces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageCorruptionException extends IOException {
  public PageCorruptionException(final String message) {
    super(message);
  }

  public PageCorruptionException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
