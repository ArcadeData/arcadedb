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

// Minimal initialization script for Gremlin Server
// Note: Most graphs are registered dynamically by ArcadeGraphManager.
// This script only provides the standard "g" alias for script-based queries.

def globals = [:]

// Lifecycle hooks for logging
globals << [hook : [
        onStartUp: { ctx ->
            ctx.logger.info("ArcadeDB Gremlin Server started.")
        },
        onShutDown: { ctx ->
            ctx.logger.info("ArcadeDB Gremlin Server stopped.")
        }
] as LifeCycleHook]

// Note: The "g" binding will be set up by ArcadeGraphManager when the default
// database is accessed. For script-based queries that use "g" directly,
// ArcadeGraphManager will provide the traversal source.
