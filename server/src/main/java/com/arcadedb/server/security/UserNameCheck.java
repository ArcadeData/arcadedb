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
package com.arcadedb.server.security;

/**
 * The shape of a user name: the one a user can be stored under and the one that is written as it is into the header a node sends
 * to a peer when it acts for the user. Two rules, one for each moment a name is used:
 * <ul>
 *   <li><b>A stored name</b> ({@link #requireStorableName}, checked wherever a user is created) has no control character, no
 *   leading or trailing whitespace and no two consecutive whitespace characters. Any other character is fine, so a name in any
 *   script, an e-mail address or a name with one inner space is stored as before.</li>
 *   <li><b>A carried name</b> ({@link #carriedNameProblem}, checked where a peer request is built) is made only of printable ASCII
 *   with the same whitespace rules: the set of names that reach the peer exactly as they were written. A name outside it is not
 *   forwarded, and the forward fails.</li>
 * </ul>
 * A user that is already stored under a name this class would refuse keeps working on the node that holds it; only creating one,
 * and naming one to a peer, is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UserNameCheck {

  private UserNameCheck() {
  }

  /**
   * @return what is wrong with {@code name} as a name to store, phrased to follow "the user name", or {@code null} when it can be
   * stored
   */
  public static String storedNameProblem(final String name) {
    return problemOf(name, false);
  }

  /**
   * @return what is wrong with {@code name} as a name written into a header, phrased to follow "the user name", or {@code null}
   * when it reaches the peer as it was written
   */
  public static String carriedNameProblem(final String name) {
    return problemOf(name, true);
  }

  /**
   * @throws IllegalArgumentException when {@code name} cannot be stored (see {@link #storedNameProblem}); the message never quotes
   *                                  the name
   */
  public static void requireStorableName(final String name) {
    final String problem = storedNameProblem(name);
    if (problem != null)
      throw new IllegalArgumentException("The user name " + problem);
  }

  private static String problemOf(final String name, final boolean carried) {
    if (name == null)
      return "is missing";

    final int last = name.length() - 1;
    boolean previousWasSpace = false;
    for (int i = 0; i <= last; i++) {
      final char c = name.charAt(i);
      if (Character.isISOControl(c))
        return "contains a control character";
      if (carried && c > 0x7e)
        return "contains a character outside printable ASCII";

      final boolean space = carried ? c == ' ' : Character.isWhitespace(c) || Character.isSpaceChar(c);
      if (space) {
        if (i == 0 || i == last)
          return "starts or ends with whitespace";
        if (previousWasSpace)
          return "contains consecutive whitespace characters";
      }
      previousWasSpace = space;
    }
    return null;
  }
}
