import { StartedTestContainer } from 'testcontainers';

declare global {
  var arcadedbContainer: StartedTestContainer;
  var arcadedbBaseURL: string;
}
