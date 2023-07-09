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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CommandSQLParsingException extends CommandParsingException {
  private String command;

  public CommandSQLParsingException(final String message) {
    super(message);
  }

  public CommandSQLParsingException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public CommandSQLParsingException setCommand(final String command) {
    this.command = command;
    return this;
  }

  public CommandSQLParsingException(final Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    if (command != null) {
      buffer.append("Exception on parsing the following SQL command: ");
      buffer.append(command);
      buffer.append("\n");
    }
    buffer.append(super.toString());
    return buffer.toString();
  }
}
