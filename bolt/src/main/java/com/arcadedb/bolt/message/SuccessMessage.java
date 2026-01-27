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
package com.arcadedb.bolt.message;

import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.Map;

/**
 * SUCCESS response message indicating successful completion.
 * Signature: 0x70
 * Fields: metadata (Map)
 */
public class SuccessMessage extends BoltMessage {
  private final Map<String, Object> metadata;

  public SuccessMessage(final Map<String, Object> metadata) {
    super(SUCCESS);
    this.metadata = metadata != null ? metadata : Map.of();
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(metadata);
  }

  @Override
  public String toString() {
    return "SUCCESS{metadata=" + metadata + "}";
  }
}
