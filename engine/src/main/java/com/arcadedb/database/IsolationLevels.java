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
package com.arcadedb.database;

import java.util.Arrays;

/**
 * The one place a transaction isolation level a CLIENT asked for becomes an engine level (issue #10386).
 * <p>
 * The rule every surface follows is that an isolation level the engine cannot provide is REFUSED: silently giving a weaker
 * guarantee than the one asked for, {@code SERIALIZABLE} answered at {@code REPEATABLE_READ}, is the one failure a client
 * cannot detect. Each surface used to implement the mapping itself - HTTP, SQL, openCypher, Postgres and gRPC - which is how
 * the gRPC one drifted into accepting it. They parse through here instead, so the set of accepted levels, and the words of
 * the refusal, are those of {@link Database.TRANSACTION_ISOLATION_LEVEL} and cannot differ from one surface to the next.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IsolationLevels {

  private IsolationLevels() {
  }

  /**
   * The engine level named exactly {@code name}, e.g. {@code REPEATABLE_READ}.
   *
   * @throws IllegalArgumentException when {@code name} is not one of the levels the engine provides, naming the ones it does
   */
  public static Database.TRANSACTION_ISOLATION_LEVEL parse(final String name) {
    if (name != null)
      for (final Database.TRANSACTION_ISOLATION_LEVEL level : Database.TRANSACTION_ISOLATION_LEVEL.values())
        if (level.name().equals(name))
          return level;
    throw refusal(name == null ? "null" : "'" + name + "'");
  }

  /**
   * The refusal of a requested level the engine does not provide. {@code requested} describes what was asked for, already
   * quoted when it is a name ({@code 'SERIALIZABLE'}) and phrased for a value that is not one ({@code of type Integer}).
   */
  public static IllegalArgumentException refusal(final String requested) {
    return new IllegalArgumentException(
        "Invalid isolationLevel " + requested + ": accepted values are " + Arrays.toString(Database.TRANSACTION_ISOLATION_LEVEL.values()));
  }
}
