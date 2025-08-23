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
