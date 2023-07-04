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
package com.arcadedb.console;

import com.arcadedb.GlobalConfiguration;

import java.util.*;
import java.util.stream.*;

public class DumpCfgApp {
  public static void main(final String[] args) {
    for (GlobalConfiguration.SCOPE scope : GlobalConfiguration.SCOPE.values())
      tableByScope(scope);

//    System.out.printf("\n|Name|Java API ENUM name|Description|Type|Default Value");
//    for (GlobalConfiguration c : GlobalConfiguration.values()) {
//      System.out.printf("\n|%s|%s|%s|%s|%s", c.getKey().substring("arcadedb".length() + 1), c.name(), c.getDescription(), c.getType().getSimpleName(),
//          c.getDefValue());
//    }
  }

  private static void tableByScope(final GlobalConfiguration.SCOPE scope) {
    System.out.printf("\n===== " + scope + "\n");

    System.out.printf("\n[%%header,cols=\"20%%,55%%,10%%,15%%\",stripes=even]");
    System.out.printf("\n|===");
    System.out.printf("\n|Name|Description|Type|Default Value");
    final List<GlobalConfiguration> orderedList = Arrays.stream(GlobalConfiguration.values()).filter((c) -> c.getScope() == scope)
        .sorted(Comparator.comparing(Enum::name)).collect(Collectors.toList());
    orderedList.sort(Comparator.comparing(Enum::name));

    for (final GlobalConfiguration c : orderedList) {
      System.out.printf("\n|`%s`|%s|%s|%s", c.getKey().substring("arcadedb".length() + 1), c.getDescription().replace("\\|", "\\\\|"),
          c.getType().getSimpleName(), c.getDefValue());
    }
    System.out.printf("\n|===");
  }
}
