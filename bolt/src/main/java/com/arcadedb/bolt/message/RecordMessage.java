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
import java.util.List;

/**
 * RECORD response message containing a data row.
 * Signature: 0x71
 * Fields: data (List of values)
 */
public class RecordMessage extends BoltMessage {
  private final List<Object> data;

  public RecordMessage(final List<Object> data) {
    super(RECORD);
    this.data = data != null ? data : List.of();
  }

  public List<Object> getData() {
    return data;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeList(data);
  }

  @Override
  public String toString() {
    return "RECORD{data=" + data + "}";
  }
}
