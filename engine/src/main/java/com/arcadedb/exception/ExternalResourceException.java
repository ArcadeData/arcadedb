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
 * An external resource a statement names - the URL of a {@code LOAD CSV} - that cannot be opened or read: a file that does not
 * exist, a host that does not answer, an archive with nothing in it. The caller's argument, so an {@link InvalidArgumentException}
 * ({@link ErrorCategory#VALIDATION}, HTTP 400), which Bolt answers with Neo4j's {@code Neo.ClientError.Statement.ExternalResourceFailed}.
 * As the plain {@link CommandExecutionException} it was raised as, no rule classified it and every protocol answered it as a server
 * fault (issue #11974).
 */
public class ExternalResourceException extends InvalidArgumentException {
  public ExternalResourceException(final String message) {
    super(message);
  }

  public ExternalResourceException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
