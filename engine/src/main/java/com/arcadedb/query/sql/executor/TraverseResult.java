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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;

/**
 * Created by luigidellaquila on 02/11/16.
 */
public class TraverseResult extends ResultInternal {
  protected Integer depth;

  public TraverseResult() {
  }

  public TraverseResult(final Document element) {
    super(element);
  }

  @Override
  public <T> T getProperty(final String name) {
    if ("$depth".equalsIgnoreCase(name))
      return (T) depth;

    return super.getProperty(name);
  }

  @Override
  public ResultInternal setProperty(final String name, final Object value) {
    if ("$depth".equalsIgnoreCase(name)) {
      if (value instanceof Number number) {
        depth = number.intValue();
      }
    } else {
      super.setProperty(name, value);
    }
    return this;
  }
}
