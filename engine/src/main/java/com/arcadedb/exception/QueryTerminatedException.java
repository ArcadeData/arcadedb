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
 * Raised inside a statement that was terminated on request ({@code terminate query <id>}, issue #9680).
 * <p>
 * Deliberately NOT a {@link TimeoutException}: a dozen engine sites catch that one and turn it into something else - the
 * rows produced so far, an empty neighbour list, a retry of the enclosing block - which is right for a deadline that a
 * statement chose for itself and wrong for an operator who asked for the work to stop. Not a {@link NeedRetryException}
 * either, or {@code Database.transaction()} would run the terminated work again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryTerminatedException extends ArcadeDBException {
  public QueryTerminatedException(final String message) {
    super(message);
  }
}
