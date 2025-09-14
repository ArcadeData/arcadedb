///
/// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { StartedTestContainer } from 'testcontainers';

// Cytoscape types for global usage
interface CytoscapeNode {
  renderedPosition(): { x: number; y: number };
  id(): string;
  data(): any;
}

interface CytoscapeElement {
  length: number;
  slice(start: number, end?: number): CytoscapeElement;
  map(fn: (element: CytoscapeNode) => any): any[];
}

interface CytoscapeInstance {
  nodes(): CytoscapeElement;
  edges(): CytoscapeElement;
  elements(selector?: string): CytoscapeElement;
  zoom(): number;
  [key: string]: any;
}

interface CytoscapeStatic {
  version: string;
  [key: string]: any;
}

declare global {
  var arcadedbContainer: StartedTestContainer;
  var arcadedbBaseURL: string;
  var globalCy: CytoscapeInstance | undefined;
  var cytoscape: CytoscapeStatic | undefined;
  var globalLayout: any;
  var globalGraphSettings: any;
  var globalNotify: any;
  var finalStats: any;
}
