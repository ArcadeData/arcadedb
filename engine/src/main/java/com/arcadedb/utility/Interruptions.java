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
package com.arcadedb.utility;

import com.arcadedb.exception.DatabaseOperationException;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

/**
 * Tells a read that an interrupt stopped from a read that failed.
 */
public final class Interruptions {
  private Interruptions() {
  }

  /**
   * Whether {@code e} is what an interrupt makes a read throw: an {@link InterruptedIOException} (the I/O lock of a page
   * load, wrapped by the read lock), a {@link ClosedByInterruptException} or an {@link InterruptedException}, anywhere in its
   * cause chain, or anything thrown by a thread that is interrupted (#10747).
   */
  public static boolean isInterruption(final Throwable e) {
    if (Thread.currentThread().isInterrupted())
      return true;
    // BOUNDED: getCause() NEVER ANSWERS THE THROWABLE ITSELF, BUT A LONGER CYCLE IS NOT RULED OUT
    Throwable t = e;
    for (int depth = 0; t != null && depth < 32; t = t.getCause(), depth++)
      if (t instanceof InterruptedIOException || t instanceof ClosedByInterruptException || t instanceof InterruptedException)
        return true;
    return false;
  }

  /**
   * Rethrows {@code e} when an interrupt stopped the read it comes from, so a read that treats any failure as a damaged record
   * does not log an interrupted one as corrupted and answer null for it (issue #12349): the interrupt goes to the reader, which
   * is what stops it. Returns otherwise.
   */
  public static void rethrowIfInterruption(final Exception e) {
    if (isInterruption(e))
      throw e instanceof RuntimeException runtime ? runtime : new DatabaseOperationException("Read interrupted", e);
  }
}
