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
package com.arcadedb.function.create;

import com.arcadedb.query.sql.executor.CommandContext;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/**
 * create.uuidBase64() - Generate a random UUID encoded as Base64.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CreateUuidBase64 extends AbstractCreateFunction {
  @Override
  protected String getSimpleName() {
    return "uuidBase64";
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Generate a random UUID encoded as Base64 (shorter than standard UUID)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final UUID uuid = UUID.randomUUID();
    final ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
  }
}
