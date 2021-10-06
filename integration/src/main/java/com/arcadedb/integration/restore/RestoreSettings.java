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
 */
package com.arcadedb.integration.restore;

import java.text.*;
import java.util.*;

public class RestoreSettings {
  public       String              format               = "full";
  public       String              databaseURL;
  public       String              url;
  public       boolean             overwriteDestination = false;
  public       int                 verboseLevel         = 2;
  public final Map<String, String> options              = new HashMap<>();

  public RestoreSettings() {
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length; )
        i += parseParameter(args[i].substring(1), i < args.length - 1 ? args[i + 1] : null);

    if (format == null)
      throw new IllegalArgumentException("Missing backup format");

    if (url == null)
      // ASSIGN DEFAULT FILENAME
      switch (format) {
      case "full":
        url = "arcadedb-backup-%s.zip";
        break;
      }

    if (url == null) {
      final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
      url = String.format(url, dateFormat.format(System.currentTimeMillis()));
    }
  }

  public int parseParameter(final String name, final String value) {
    if ("format".equals(name))
      format = value.toLowerCase();
    else if ("f".equals(name))
      url = value;
    else if ("d".equals(name))
      databaseURL = value;
    else if ("o".equals(name)) {
      overwriteDestination = true;
      return 1;
    } else
      // ADDITIONAL OPTIONS
      options.put(name, value);
    return 2;
  }
}
