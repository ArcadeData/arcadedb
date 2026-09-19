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
 * The caller named a database that does not exist: there is no directory for it to open. Permanent and the
 * caller's - no retry, no wait and no operator action on this server makes the same request succeed - which is
 * what separates it from its parent {@link DatabaseNotAvailableException}, raised for a database that exists but
 * is closed, dropped mid-request or held back by an unfinished snapshot install.
 * <p>
 * It is a subtype rather than a sibling so every caller that already answers "not there" for the parent - the
 * HTTP handler's 404 among them - keeps answering it, while a caller that has a finer answer for a name that was
 * never right (a Bolt client is told {@code Neo.ClientError.Database.DatabaseNotFound} rather than the generic
 * database error a Neo4j driver logs as an internal server fault) can ask for the narrower type. Before it
 * existed the condition was a bare {@link DatabaseOperationException} whose only distinguishing feature was the
 * wording of its message, so no protocol could answer it without matching on prose (issue #7874).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseNotFoundException extends DatabaseNotAvailableException {
  public DatabaseNotFoundException(final String s) {
    super(s);
  }
}
