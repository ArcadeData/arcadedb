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
package com.arcadedb.log;

import java.util.logging.Level;

/**
 * Logger interface that avoids using varargs to remove garbage on the GC
 */
public interface Logger {
  void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2, Object arg3, Object arg4,
      Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14, Object arg15,
      Object arg16, Object arg17);

  void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args);

  void flush();
}
