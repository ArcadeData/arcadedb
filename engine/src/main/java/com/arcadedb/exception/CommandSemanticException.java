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
 * Thrown when a query is syntactically valid but violates a semantic rule (for example an undefined
 * variable, a type conflict, or an invalid aggregation). Extends CommandParsingException so existing
 * handlers keep treating it as a parsing error, while callers that care can distinguish it.
 */
public class CommandSemanticException extends CommandParsingException {
  public CommandSemanticException(final String message) {
    super(message);
  }

  public CommandSemanticException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public CommandSemanticException(final Throwable cause) {
    super(cause);
  }
}
