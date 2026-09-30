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
package com.arcadedb;

import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The identifier ArcadeDB generates for an instance (standalone server, HA node or embedded engine) so support can tell
 * which instance is talking. The customer copies it from the log or from the server information into the support portal.
 * <p>
 * Format: {@code adb-} followed by a canonical lowercase UUID, exactly 40 characters, e.g.
 * {@code adb-123e4567-e89b-12d3-a456-426614174000}. It is NOT a credential and is never used for authentication.
 * <p>
 * The server persists it in the file {@code instance.id} of its configuration directory. That file is node-local: it is
 * never replicated between HA nodes. Note that copying a configuration directory to create a new node duplicates the
 * id, so delete {@code instance.id} from the copy (or set {@code arcadedb.instance.id}) before starting it.
 */
public final class InstanceId {
  public static final String PREFIX = "adb-";

  private static final Pattern PATTERN = Pattern.compile(
      "^adb-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  private InstanceId() {
  }

  /**
   * Tells whether {@code value} is exactly a canonical (lowercase) instance id. No trimming or case folding is applied,
   * use {@link #normalize(String)} first.
   */
  public static boolean isValid(final String value) {
    return value != null && PATTERN.matcher(value).matches();
  }

  /**
   * Trims and lower-cases {@code value}. Returns the normalized id, or null when the result is not a valid instance id.
   */
  public static String normalize(final String value) {
    if (value == null)
      return null;
    final String normalized = value.trim().toLowerCase(Locale.ROOT);
    return isValid(normalized) ? normalized : null;
  }

  public static String generate() {
    return PREFIX + UUID.randomUUID();
  }
}
