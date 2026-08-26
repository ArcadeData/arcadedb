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
package com.arcadedb.console;

public class ConsoleException extends RuntimeException {
  private final boolean alreadyReported;

  public ConsoleException(final String message) {
    this(message, false);
  }

  /**
   * @param alreadyReported true when the thrower already sent this error to the console output, so a generic catch further up
   *                        the call chain - for example the one wrapping every command in {@code Console.execute(String)} -
   *                        must not report it a second time (issue #6439).
   */
  public ConsoleException(final String message, final boolean alreadyReported) {
    super(message);
    this.alreadyReported = alreadyReported;
  }

  public boolean isAlreadyReported() {
    return alreadyReported;
  }
}
