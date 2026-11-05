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
 * A number that the numeric type it is converted to cannot hold: a LONG beyond the INTEGER range, a DOUBLE that overflows a
 * FLOAT, a BigDecimal beyond 64 bits. Still the {@link IllegalArgumentException} the conversion always raised, so every caller
 * that catches that keeps working; its own type is what lets a protocol tell it from the other refusals of a conversion, e.g.
 * the Postgres wire answers it {@code 22003} numeric_value_out_of_range (issue #11314).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ValueOutOfRangeException extends IllegalArgumentException {
  public ValueOutOfRangeException(final String message) {
    super(message);
  }
}
