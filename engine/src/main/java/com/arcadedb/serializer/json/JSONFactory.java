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
package com.arcadedb.serializer.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * JSON object.<br>
 * This API is compatible with org.json Java API, but uses Google GSON library under the hood. The main reason why we created this wrapper is
 * because the maintainer of the project org.json are not open to support ordered attributes as an option.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONFactory {
  private final Gson gson;
  private final Gson gsonPrettyPrint;

  public final static JSONFactory INSTANCE = new JSONFactory();

  private JSONFactory() {
    gson = new GsonBuilder()//
        .serializeNulls()//
        .disableHtmlEscaping()// Prevents converting & to \u0026, < to \u003c, etc. (#1602)
        .create();

    gsonPrettyPrint = new GsonBuilder()//
        .serializeNulls()//
        .setPrettyPrinting()//
        .disableHtmlEscaping()// Prevents converting & to \u0026, < to \u003c, etc. (#1602)
        .create();
  }

  public Gson getGson() {
    return gson;
  }

  public Gson getGsonPrettyPrint() {
    return gsonPrettyPrint;
  }
}
