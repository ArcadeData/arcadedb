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
 * A construct the grammar accepts and the engine refuses by name (issue #9940). Its own type, rather than a message
 * convention, is what tells a refusal from any other parse failure - a subquery body that cannot be built falls back to its
 * text, a refusal must not - and the message keeps the {@code FeatureNotImplemented: } prefix clients see.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FeatureNotImplementedException extends CommandParsingException {
  public FeatureNotImplementedException(final String message) {
    super("FeatureNotImplemented: " + message);
  }
}
