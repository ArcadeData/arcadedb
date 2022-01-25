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
package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;

import java.util.Map;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public abstract class ArcadeGraphSON extends TinkerPopJacksonModule {

  public ArcadeGraphSON(String name) {
    super(name);
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return null;
  }

  @Override
  public String getTypeNamespace() {
    return "arcade";
  }
}
