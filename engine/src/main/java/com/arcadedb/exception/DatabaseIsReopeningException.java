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
 * The handle a request is using was closed so that the same database could be reopened under its name - an HA
 * snapshot reinstall swapping the files of a lagging replica, for instance - rather than closed for good. Transient by
 * construction: the database resolved again a moment later is the reopened one, so every wire protocol reports it as
 * retryable ({@link ErrorCategory#RETRY}, HTTP 503).
 * <p>
 * It extends {@link DatabaseIsClosedException} so everything that already handles a closed database keeps handling
 * this one. A plain {@link DatabaseIsClosedException} is the permanent case - a {@code DROP DATABASE}, a
 * {@code CLOSE DATABASE}, a server shutdown - which no retry against the same name can fix (issue #8199).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseIsReopeningException extends DatabaseIsClosedException {
  public DatabaseIsReopeningException(final String s) {
    super(s);
  }
}
