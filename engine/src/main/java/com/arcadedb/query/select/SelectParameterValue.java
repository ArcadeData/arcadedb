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
package com.arcadedb.query.select;

import com.arcadedb.database.Document;

public class SelectParameterValue implements SelectRuntimeValue {
  public final  String parameterName;
  private final Select select;

  public SelectParameterValue(final Select select, final String parameterName) {
    this.select = select;
    this.parameterName = parameterName;
  }

  @Override
  public Object eval(final Document record) {
    if (select.parameters == null)
      throw new IllegalArgumentException("Missing parameter '" + parameterName + "'");
    if (!select.parameters.containsKey(parameterName))
      throw new IllegalArgumentException("Missing parameter '" + parameterName + "'");
    return select.parameters.get(parameterName);
  }

  @Override
  public String toString() {
    return "#" + parameterName;
  }
}
