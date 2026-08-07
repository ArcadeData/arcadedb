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

public class ConcurrentModificationException extends NeedRetryException {
  public ConcurrentModificationException(final String s) {
    super(s);
  }

  /**
   * #5764: a conflict raised FROM a caught exception keeps it as the cause. Most of these are absorbed by the
   * transaction retry and never seen, so the single run that does surface one is the retry-exhausted run - i.e.
   * exactly the run whose stack trace has to be diagnosable, and the one that used to arrive with the original
   * failure discarded. {@link NeedRetryException} already declared the pair; only this subclass was missing it.
   */
  public ConcurrentModificationException(final String s, final Throwable cause) {
    super(s, cause);
  }
}
