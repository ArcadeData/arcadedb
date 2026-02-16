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
package com.arcadedb.function.util;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * util.md5(value) - Compute MD5 hash.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilMd5 extends AbstractUtilFunction {
  @Override
  protected String getSimpleName() {
    return "md5";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Compute MD5 hash of the given value";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    return computeHash(args[0], "MD5");
  }
}
