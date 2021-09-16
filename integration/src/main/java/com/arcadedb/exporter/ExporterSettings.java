/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.exporter;

import java.util.HashMap;
import java.util.Map;

public class ExporterSettings {
  public       String              databaseURL;
  public       String              file         = "arcadedb-export.tgz";
  public       int                 verboseLevel = 2;
  public final Map<String, String> options      = new HashMap<>();

  public ExporterSettings() {
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        parseParameter(args[i].substring(1), args[i + 1]);
  }

  public void parseParameter(final String name, final String value) {
    if ("file".equals(name))
      file = value;
    else if ("database".equals(name))
      databaseURL = value;
    else
      // ADDITIONAL OPTIONS
      options.put(name, value);
  }
}
