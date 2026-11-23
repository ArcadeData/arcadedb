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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;

/**
 * A pattern expression used where its value is not a boolean, such as {@code size((p)-->())} (issue #11888). Its own type,
 * like {@link FeatureNotImplementedException}, so that the AST build of a subquery body, which falls back to the body text
 * on any other parse failure, lets the refusal through: the text fallback would defer it to execution, and an EXISTS that
 * the boolean simplifier drops (for example {@code true OR EXISTS { ... }}) would never report it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class NonBooleanPatternExpressionException extends CommandParsingException {
  public NonBooleanPatternExpressionException(final String message) {
    super("UnexpectedSyntax: " + message);
  }
}
