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
package com.arcadedb.mcp;

/**
 * A tool call the MCP layer itself refuses for its arguments ("'database' is required", "Unknown tool", a limit out of
 * range): text this server words about the request, which carries no stored data and is what the caller needs to fix
 * it, so it is answered in every server mode. Any other failure of a tool - an {@link IllegalArgumentException} the
 * engine raises included - is engine text, which production mode conceals (issue #8749). Telling the two apart by the
 * JDK exception class would let an engine message through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPToolArgumentException extends IllegalArgumentException {
  public MCPToolArgumentException(final String message) {
    super(message);
  }
}
