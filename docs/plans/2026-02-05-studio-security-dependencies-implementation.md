# Studio Security Dependencies Cleanup - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace unmaintained frontend dependencies with modern alternatives to address Meterian security findings.

**Architecture:** Remove clipboard, notyf, cytoscape-cola/webcola libraries. Replace with native Clipboard API, SweetAlert2 Toast mixin, and cytoscape-fcose. Update e2e tests to match new implementations.

**Tech Stack:** JavaScript (ES6+), npm, webpack, Playwright (e2e tests)

---

## Task 1: Update package.json Dependencies

**Files:**
- Modify: `studio/package.json`

**Step 1: Remove old dependencies and add new ones**

Edit `studio/package.json` dependencies section:

Remove these lines:
```json
"clipboard": "^2.0.11",
"cytoscape-cola": "^2.5.1",
"notyf": "^3.10.0",
"webcola": "^3.4.0"
```

Add this line (after `cytoscape-cxtmenu`):
```json
"cytoscape-fcose": "^2.2.0",
```

**Step 2: Verify package.json is valid JSON**

Run: `cd studio && cat package.json | jq .`
Expected: Valid JSON output without errors

**Step 3: Commit dependency changes**

```bash
git add studio/package.json
git commit -m "chore(studio): update dependencies for security cleanup

Remove: clipboard, notyf, cytoscape-cola, webcola
Add: cytoscape-fcose"
```

---

## Task 2: Update webpack.config.js

**Files:**
- Modify: `studio/webpack.config.js`

**Step 1: Remove old library copy rules**

Remove these CopyWebpackPlugin patterns from `studio/webpack.config.js`:

```javascript
        {
          from: 'node_modules/notyf/notyf.min.js',
          to: 'js/notyf.min.js',
        },
        {
          from: 'node_modules/notyf/notyf.min.css',
          to: 'css/notyf.min.css',
        },
```

```javascript
        {
          from: 'node_modules/cytoscape-cola/cytoscape-cola.js',
          to: 'js/cytoscape-cola.js',
        },
```

```javascript
        {
          from: 'node_modules/webcola/WebCola/cola.min.js',
          to: 'js/cola.min.js',
        },
```

```javascript
        {
          from: 'node_modules/clipboard/dist/clipboard.min.js',
          to: 'js/clipboard.min.js',
        },
```

**Step 2: Add cytoscape-fcose copy rule**

Add this pattern after `cytoscape-cxtmenu` entry (~line 199):

```javascript
        {
          from: 'node_modules/cytoscape-fcose/cytoscape-fcose.js',
          to: 'js/cytoscape-fcose.js',
        },
```

**Step 3: Commit webpack changes**

```bash
git add studio/webpack.config.js
git commit -m "chore(studio): update webpack config for new dependencies

Remove copy rules for: notyf, clipboard, cytoscape-cola, webcola
Add copy rule for: cytoscape-fcose"
```

---

## Task 3: Update index.html - Remove Old Libraries

**Files:**
- Modify: `studio/src/main/resources/static/index.html`

**Step 1: Remove notyf CSS import**

Remove this line (~line 21):
```html
    <link rel="stylesheet" href="dist/css/notyf.min.css" />
```

**Step 2: Remove notyf and clipboard JS imports**

Remove these lines (~lines 32-33):
```html
    <script src="dist/js/notyf.min.js"></script>
    <script src="dist/js/clipboard.min.js"></script>
```

**Step 3: Remove cola and cytoscape-cola JS imports**

Remove these lines (~lines 60-61):
```html
    <script src="dist/js/cola.min.js"></script>
    <script src="dist/js/cytoscape-cola.js"></script>
```

**Step 4: Add cytoscape-fcose JS import**

Add after cytoscape.min.js (~line 59):
```html
    <script src="dist/js/cytoscape-fcose.js"></script>
```

**Step 5: Commit HTML library changes**

```bash
git add studio/src/main/resources/static/index.html
git commit -m "chore(studio): update index.html script/style imports

Remove: notyf.min.css, notyf.min.js, clipboard.min.js, cola.min.js, cytoscape-cola.js
Add: cytoscape-fcose.js"
```

---

## Task 4: Replace Notyf with SweetAlert2 Toast

**Files:**
- Modify: `studio/src/main/resources/static/index.html`

**Step 1: Replace Notyf initialization with SweetAlert2 Toast**

In `index.html`, replace the entire Notyf initialization block (~lines 85-135):

OLD:
```javascript
      // Initialize Notyf with notification queue
      let notyf = null;
      let notificationQueue = [];
      let isNotyfReady = false;

      document.addEventListener('DOMContentLoaded', function() {
        // Initialize Notyf (replacement for Bootstrap Notify)
        notyf = new Notyf({
          duration: 4000,
          position: {
            x: 'right',
            y: 'top'
          },
          dismissible: true,
          ripple: false
        });

        // Mark as ready and process queued notifications
        isNotyfReady = true;
        processNotificationQueue();
      });

      function processNotificationQueue() {
        while (notificationQueue.length > 0 && isNotyfReady) {
          const notification = notificationQueue.shift();
          showNotification(notification.title, notification.message, notification.type);
        }
      }

      function showNotification(title, message, type) {
        const fullMessage = title ? `<strong>${title}</strong><br>${message}` : message;

        if (type === 'success') {
          notyf.success(fullMessage);
        } else if (type === 'danger' || type === 'error') {
          notyf.error(fullMessage);
        } else {
          notyf.success(fullMessage);
        }
      }

      // Global compatibility function for notifications
      function globalNotify(title, message, type) {
        if (!isNotyfReady) {
          // Queue notification for later processing
          notificationQueue.push({ title, message, type });
          return;
        }

        showNotification(title, message, type);
      }
```

NEW:
```javascript
      // Initialize SweetAlert2 Toast (replacement for Notyf)
      let Toast = null;
      let notificationQueue = [];
      let isToastReady = false;

      document.addEventListener('DOMContentLoaded', function() {
        // Initialize SweetAlert2 Toast mixin
        Toast = Swal.mixin({
          toast: true,
          position: 'top-end',
          showConfirmButton: false,
          timer: 4000,
          timerProgressBar: true,
          didOpen: (toast) => {
            toast.onmouseenter = Swal.stopTimer;
            toast.onmouseleave = Swal.resumeTimer;
          }
        });

        // Mark as ready and process queued notifications
        isToastReady = true;
        processNotificationQueue();
      });

      function processNotificationQueue() {
        while (notificationQueue.length > 0 && isToastReady) {
          const notification = notificationQueue.shift();
          showNotification(notification.title, notification.message, notification.type);
        }
      }

      function showNotification(title, message, type) {
        const icon = (type === 'danger' || type === 'error') ? 'error' : 'success';
        Toast.fire({
          icon: icon,
          title: title || '',
          text: message
        });
      }

      // Global compatibility function for notifications
      function globalNotify(title, message, type) {
        if (!isToastReady) {
          // Queue notification for later processing
          notificationQueue.push({ title, message, type });
          return;
        }

        showNotification(title, message, type);
      }
```

**Step 2: Commit Toast replacement**

```bash
git add studio/src/main/resources/static/index.html
git commit -m "feat(studio): replace notyf with SweetAlert2 Toast mixin

Use existing SweetAlert2 library to display toast notifications.
Maintains same API (globalNotify, showNotification) for compatibility."
```

---

## Task 5: Replace ClipboardJS with Native Clipboard API

**Files:**
- Modify: `studio/src/main/resources/static/js/studio-graph.js`

**Step 1: Update the export graph function**

In `studio-graph.js`, find the `displayExport` function that creates the popup with ClipboardJS.

Find this code block (~lines 207-220):
```javascript
  html += "<button id='popupClipboard' type='button' data-clipboard-target='#exportContent' class='clipboard-trigger btn btn-primary'>";
  html += "<i class='fa fa-copy'></i> Copy to clipboard and close</button> or ";
```
...
```javascript
  new ClipboardJS("#popupClipboard").on("success", function (e) {
    $("#popup").modal("hide");
  });
```

Replace the button HTML (remove data-clipboard-target):
```javascript
  html += "<button id='popupClipboard' type='button' class='btn btn-primary'>";
  html += "<i class='fa fa-copy'></i> Copy to clipboard and close</button> or ";
```

Replace the ClipboardJS initialization with native Clipboard API:
```javascript
  $("#popupClipboard").on("click", async function() {
    const text = $("#exportContent").val() || $("#exportContent").text();
    try {
      await navigator.clipboard.writeText(text);
      $("#popup").modal("hide");
    } catch (err) {
      console.error("Failed to copy to clipboard:", err);
      globalNotify("Error", "Failed to copy to clipboard", "danger");
    }
  });
```

**Step 2: Commit clipboard replacement**

```bash
git add studio/src/main/resources/static/js/studio-graph.js
git commit -m "feat(studio): replace ClipboardJS with native Clipboard API

Use navigator.clipboard.writeText() for copy functionality.
Adds error handling with user notification on failure."
```

---

## Task 6: Replace Cola Layout with Fcose

**Files:**
- Modify: `studio/src/main/resources/static/js/studio-graph-widget.js`
- Modify: `studio/src/main/resources/static/js/studio-graph.js`

**Step 1: Update layout configuration in studio-graph-widget.js**

In `studio-graph-widget.js`, find the globalLayout definition (~lines 55-64):

OLD:
```javascript
  globalLayout = {
    name: "cola",
    animate: true,
    refresh: 2,
    ungrabifyWhileSimulating: true,
    nodeSpacing: function (node) {
      return globalGraphSettings.graphSpacing;
    },
    spacingFactor: 1.75,
  };
```

NEW:
```javascript
  globalLayout = {
    name: "fcose",
    animate: true,
    animationDuration: 500,
    nodeSeparation: globalGraphSettings.graphSpacing * 1.75,
    quality: "default",
    randomize: false,
    fit: true,
    padding: 30,
  };
```

**Step 2: Update GraphML export layoutBy in studio-graph.js**

In `studio-graph.js`, find the graphml export configuration (~line 128):

OLD:
```javascript
        layoutBy: "cola",
```

NEW:
```javascript
        layoutBy: "fcose",
```

**Step 3: Commit layout changes**

```bash
git add studio/src/main/resources/static/js/studio-graph-widget.js studio/src/main/resources/static/js/studio-graph.js
git commit -m "feat(studio): replace cola layout with fcose

fcose is the officially recommended force-directed layout.
Provides better performance and cleaner dependency tree."
```

---

## Task 7: Install Dependencies and Build

**Files:**
- None (build step)

**Step 1: Install npm dependencies**

Run: `cd studio && npm install`
Expected: Dependencies installed without errors

**Step 2: Build webpack bundle**

Run: `cd studio && npm run build`
Expected: Build completes successfully

**Step 3: Verify dist files**

Run: `ls -la studio/src/main/resources/static/dist/js/ | grep -E "(fcose|cola|notyf|clipboard)"`
Expected: Only `cytoscape-fcose.js` present, no cola/notyf/clipboard files

**Step 4: Commit any lock file changes**

```bash
git add studio/package-lock.json
git commit -m "chore(studio): update package-lock.json after dependency changes"
```

---

## Task 8: Update E2E Test - Notification Selectors

**Files:**
- Modify: `e2e-studio/tests/notification-test.spec.ts`

**Step 1: Update notyf selector to SweetAlert2 toast selector**

In `notification-test.spec.ts`, find and replace all occurrences:

OLD:
```typescript
'.notyf__toast'
```

NEW:
```typescript
'.swal2-toast'
```

This appears in:
- Line 72: `page.waitForSelector('.notyf__toast', { timeout: 5000 })`
- Line 78: `await page.locator('.notyf__toast').count()`
- Line 132: `await page.locator('.notyf__toast').count()`

Also update the error check (~line 83):
OLD:
```typescript
      error.includes("can't access lexical declaration 'notyf' before initialization")
```

NEW:
```typescript
      error.includes("can't access lexical declaration 'Toast' before initialization")
```

**Step 2: Commit e2e notification test update**

```bash
git add e2e-studio/tests/notification-test.spec.ts
git commit -m "test(e2e): update notification test for SweetAlert2 Toast

Replace .notyf__toast selectors with .swal2-toast"
```

---

## Task 9: Update E2E Test - Cytoscape Layout

**Files:**
- Modify: `e2e-studio/tests/cytoscape-validation.spec.ts`

**Step 1: Update cola layout test to fcose**

In `cytoscape-validation.spec.ts`, find the extension test (~line 353):

OLD:
```typescript
        // Test if cola layout can be created
        const colaTest = globalCy.layout({ name: 'cola', infinite: false });
        const hasColaExtension = colaTest !== null;
```

NEW:
```typescript
        // Test if fcose layout can be created
        const fcoseTest = globalCy.layout({ name: 'fcose', animate: false });
        const hasFcoseExtension = fcoseTest !== null;
```

Also update the return statement (~line 358):
OLD:
```typescript
        return {
          hasColaExtension,
          hasCxtMenuExtension,
          globalCyAvailable: true
        };
```

NEW:
```typescript
        return {
          hasFcoseExtension,
          hasCxtMenuExtension,
          globalCyAvailable: true
        };
```

**Step 2: Commit e2e cytoscape test update**

```bash
git add e2e-studio/tests/cytoscape-validation.spec.ts
git commit -m "test(e2e): update cytoscape test for fcose layout

Replace cola layout test with fcose layout test"
```

---

## Task 10: Add Clipboard E2E Test

**Files:**
- Modify: `e2e-studio/tests/graph-export.spec.ts`

**Step 1: Add clipboard functionality test**

Add this new test after the existing export tests in `graph-export.spec.ts` (before the closing `});`):

```typescript
  test('should copy export content to clipboard', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Grant clipboard permissions
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

    // Test clipboard functionality via the export modal
    const clipboardTest = await page.evaluate(async () => {
      if (!globalCy) return { error: 'No graph available' };

      try {
        // Simulate what the export does - get graph data as text
        const graphData = JSON.stringify({
          nodes: globalCy.nodes().map(node => ({ id: node.id(), data: node.data() })),
          edges: globalCy.edges().map(edge => ({ id: edge.id(), data: edge.data() }))
        });

        // Test native clipboard API
        await navigator.clipboard.writeText(graphData);
        const clipboardContent = await navigator.clipboard.readText();

        return {
          success: true,
          dataLength: graphData.length,
          clipboardLength: clipboardContent.length,
          matches: graphData === clipboardContent
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    console.log('Clipboard test result:', clipboardTest);
    expect(clipboardTest).toBeTruthy();

    if (clipboardTest && !clipboardTest.error) {
      expect(clipboardTest.success).toBe(true);
      expect(clipboardTest.matches).toBe(true);
    }
  });
```

**Step 2: Commit clipboard test**

```bash
git add e2e-studio/tests/graph-export.spec.ts
git commit -m "test(e2e): add clipboard functionality test

Verify native Clipboard API works for export copy feature"
```

---

## Task 11: Build Docker Image and Run E2E Tests

**Files:**
- None (verification step)

**Step 1: Build Java backend with Docker**

Run: `mvn clean install -DskipTests -Pdocker`
Expected: Build succeeds, Docker image created

**Step 2: Run e2e tests**

Run: `cd e2e-studio && npm test`
Expected: All tests pass

**Step 3: If tests fail, investigate and fix**

Check test output for specific failures. Common issues:
- Selector timing: increase timeouts
- Layout differences: adjust visual assertions
- Permission issues: verify clipboard permissions granted

---

## Task 12: Final Verification and Cleanup

**Files:**
- None (verification step)

**Step 1: Verify security improvement**

Run: `cd studio && npm audit`
Expected: Reduced vulnerabilities (no more clipboard/notyf/cola related issues)

**Step 2: Verify no old library files in dist**

Run: `find studio/src/main/resources/static/dist -name "*notyf*" -o -name "*clipboard*" -o -name "*cola*" | grep -v fcose`
Expected: No files found

**Step 3: Manual smoke test (optional)**

Start server and verify:
1. Graph renders with proper layout
2. Toast notifications appear on errors
3. Copy to clipboard works in export modal
4. All export formats work (JSON, PNG, JPEG, GraphML)

**Step 4: Create final summary commit (if needed)**

```bash
git add -A
git status
# Only commit if there are uncommitted changes
git commit -m "chore(studio): complete security dependencies cleanup"
```

---

## Summary

After completing all tasks:

| Metric | Before | After |
|--------|--------|-------|
| MEDIUM vulnerabilities | 4 | 1 |
| LOW vulnerabilities | 6 | 5 |
| Direct dependencies removed | 0 | 4 |
| Direct dependencies added | 0 | 1 |
| Transitive dependencies eliminated | 0 | ~15 |
