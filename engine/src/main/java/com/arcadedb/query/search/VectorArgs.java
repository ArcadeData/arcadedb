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
package com.arcadedb.query.search;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Argument helpers shared by the search services in this package. The wording of the faults matters as much
 * as the check: it is the same string on every protocol surface, so a caller that moved from MCP to HTTP or
 * gRPC reads the same message for the same mistake.
 */
public final class VectorArgs {
  private VectorArgs() {
  }

  /**
   * Returns a required string argument, throwing an {@link IllegalArgumentException} that names the field when
   * it is absent, null or blank. Every HTTP handler in this package maps that exception to a 400.
   */
  public static String requireString(final JSONObject args, final String field) {
    final String value = args.getString(field, null);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("'" + field + "' is required");
    return value;
  }
}
