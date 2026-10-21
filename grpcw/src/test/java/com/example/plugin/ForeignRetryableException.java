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
package com.example.plugin;

import com.arcadedb.exception.NeedRetryException;

/**
 * Stands for a retryable type a plugin or library defines outside ArcadeDB's packages (issue #10143): the admin service
 * must not send its class name to the client.
 */
public class ForeignRetryableException extends NeedRetryException {
  public ForeignRetryableException(final String message) {
    super(message);
  }
}
