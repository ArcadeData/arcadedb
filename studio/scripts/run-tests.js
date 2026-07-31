#!/usr/bin/env node
/**
 * Runs every studio/test/*.test.js file through Node's built-in test runner.
 *
 * The files are enumerated here and passed explicitly rather than relying on `node --test test/`
 * or `node --test "test/*.test.js"`: directory arguments and glob patterns are each supported only
 * by a subset of the Node releases the build can end up on (the frontend-maven-plugin pins its own
 * version, developers run whatever they have installed), while an explicit file list works on all
 * of them and does not depend on shell globbing either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const TEST_DIR = path.join(__dirname, '..', 'test');

const files = fs
  .readdirSync(TEST_DIR)
  .filter((f) => f.endsWith('.test.js'))
  .sort()
  .map((f) => path.join(TEST_DIR, f));

if (files.length === 0) {
  console.error('No test files found in ' + TEST_DIR);
  process.exit(1);
}

const result = spawnSync(process.execPath, ['--test'].concat(files), { stdio: 'inherit' });

if (result.error) {
  console.error(result.error);
  process.exit(1);
}

process.exit(result.status === null ? 1 : result.status);
