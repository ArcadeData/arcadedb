#!/usr/bin/env node
//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
// SPDX-License-Identifier: Apache-2.0
// Self-test for the PR-triage scripts in .github/scripts (classify-pr.yml):
//
//   issue-workflow-config.js  the triage labels, issue-link labels and caps they share
//
// The apply script acts on what a model returned after reading an untrusted PR, so the cases
// pin both directions: what must be applied, and what must not. Case groups are registered in
// `cases` below and run in order; add new groups there.
//
// The GitHub client is a fake: every call is recorded rather than made.
//
// Usage:
//   test-pr-workflow-scripts.js

"use strict";

const path = require("path");

const SCRIPTS = path.join(__dirname, "..");
const config = require(path.join(SCRIPTS, "issue-workflow-config.js"));

let failures = 0;
let checks = 0;

const check = (label, actual, expected) => {
  checks++;
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) {
    failures++;
    console.log(`  FAIL ${label}\n       expected ${e}\n       actual   ${a}`);
  }
};

// Records every call instead of making it; later cases use it to assert nothing was written.
const recorder = () => {
  const log = [];
  const call = (name) => async (args) => {
    log.push({ name, args });
    return { data: [] };
  };
  const github = {
    rest: {
      issues: {
        addLabels: call("addLabels"),
        removeLabel: call("removeLabel"),
        createLabel: call("createLabel"),
        createComment: call("createComment"),
        updateComment: call("updateComment"),
      },
    },
  };
  return { log, github };
};

const configCases = () => {
  const levels = config.TRIAGE_LEVELS;
  check("config: triage level names", levels?.map((l) => l.name), [
    "triage:lgtm", "triage:needs-changes", "triage:unsafe",
  ]);
  check("config: triage verdicts", levels?.map((l) => l.verdict), ["lgtm", "needs-changes", "unsafe"]);
  check("config: triage colours", levels?.map((l) => l.color), ["0E8A16", "D93F0B", "B60205"]);
  check("config: triage descriptions are set", levels?.every((l) => l.description?.length > 0), true);
  const link = config.ISSUE_LINK_LABELS;
  check("config: has-issue", [link?.has?.name, link?.has?.color], ["has-issue", "0075CA"]);
  check("config: no-issue", [link?.none?.name, link?.none?.color], ["no-issue", "C5DEF5"]);
  check("config: link descriptions are set", [link?.has?.description, link?.none?.description].every((d) => d?.length > 0), true);
  check("config: marker prefix", config.PR_MARKER_PREFIX, "<!-- classify-pr");
  check("config: review limit", config.PR_REVIEW_LIMIT, 60000);
  const five = ["triage:lgtm", "triage:needs-changes", "triage:unsafe", "has-issue", "no-issue"];
  check("config: new labels are not module labels", five.map((n) => config.NON_MODULE_LABELS.includes(n)), five.map(() => true));
};

const cases = [configCases];

(async () => {
  void recorder;
  for (const group of cases) await group();
  console.log(`\n${checks} checks, ${failures} failed`);
  process.exit(failures === 0 ? 0 : 1);
})();
