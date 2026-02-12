# Studio Frontend Security Dependencies Cleanup

**Date:** 2026-02-05
**Status:** Approved
**Branch:** fix-security

## Problem Statement

The Meterian security report flagged 37 dependencies in the studio module:
- 4 MEDIUM severity (unmaintained libraries 8-9+ years old)
- 6 LOW severity (unmaintained libraries 6.5-7.9 years old)
- 27 informational (potentially stale libraries)

Root cause: Several direct dependencies pull in old, unmaintained transitive dependencies.

## Decision

Comprehensive cleanup addressing all flagged libraries through consolidation and native API replacement.

## Changes

### Dependencies to Remove

| Library | Version | Issues | Replacement |
|---------|---------|--------|-------------|
| `clipboard` | 2.0.11 | 4 MEDIUM + 1 LOW transitive deps | Native Clipboard API |
| `notyf` | 3.10.0 | Unmaintained 56 months | SweetAlert2 Toast Mixin (already in project) |
| `cytoscape-cola` | 2.5.1 | 7 old d3-* transitive deps | `cytoscape-fcose` |
| `webcola` | 3.4.0 | Transitive of cytoscape-cola | Removed with cytoscape-cola |

### Dependencies to Add

| Library | Version | Rationale |
|---------|---------|-----------|
| `cytoscape-fcose` | ^2.2.0 | Officially recommended layout, cleaner deps |

### Dependencies to Keep (Accepted Risk)

| Library | Version | Severity | Rationale |
|---------|---------|----------|-----------|
| `jszip` | 3.10.1 | 1 MEDIUM + 4 LOW | Required for DataTables Excel export |
| `cytoscape-graphml` | 1.0.6 | 1 LOW | Used for GraphML export, no alternative |

## Implementation Details

### 1. Clipboard Replacement

**Current** (`studio-graph.js`):
```javascript
new ClipboardJS("#popupClipboard").on("success", function (e) {
  $("#popup").modal("hide");
});
```

**New**:
```javascript
$("#popupClipboard").on("click", async function() {
  const text = $("#exportContent").val() || $("#exportContent").text();
  try {
    await navigator.clipboard.writeText(text);
    $("#popup").modal("hide");
  } catch (err) {
    console.error("Failed to copy:", err);
    globalNotify("Error", "Failed to copy to clipboard", "danger");
  }
});
```

### 2. Notyf Replacement with SweetAlert2 Toast

**Current** (`index.html`):
```javascript
notyf = new Notyf({
  duration: 4000,
  position: { x: 'right', y: 'top' },
  dismissible: true,
  ripple: false
});

function showNotification(title, message, type) {
  const fullMessage = title ? `<strong>${title}</strong><br>${message}` : message;
  if (type === 'success') notyf.success(fullMessage);
  else if (type === 'danger' || type === 'error') notyf.error(fullMessage);
  else notyf.success(fullMessage);
}
```

**New**:
```javascript
const Toast = Swal.mixin({
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

function showNotification(title, message, type) {
  const icon = (type === 'danger' || type === 'error') ? 'error' : 'success';
  Toast.fire({
    icon: icon,
    title: title || '',
    text: message
  });
}
```

### 3. Cytoscape-Cola to Fcose Migration

**Current** (`studio-graph-widget.js`):
```javascript
globalLayout = {
  name: "cola",
  animate: true,
  refresh: 2,
  ungrabifyWhileSimulating: true,
  nodeSpacing: function(node) { return globalGraphSettings.graphSpacing; },
  spacingFactor: 1.75,
};
```

**New**:
```javascript
globalLayout = {
  name: "fcose",
  animate: true,
  animationDuration: 500,
  nodeSeparation: globalGraphSettings.graphSpacing * 1.75,
  quality: "default",
  randomize: false,
};
```

## Files to Modify

### Frontend Code
- `studio/package.json` - Update dependencies
- `studio/src/main/resources/static/index.html` - Remove notyf/clipboard, update Toast init
- `studio/src/main/resources/static/js/studio-graph.js` - Replace ClipboardJS with native API
- `studio/src/main/resources/static/js/studio-graph-widget.js` - Change layout cola → fcose
- `studio/webpack.config.js` - Remove notyf/clipboard copy rules (if present)

### E2E Tests
- `e2e-studio/tests/notification-test.spec.ts` - Update `.notyf__toast` → `.swal2-toast`
- `e2e-studio/tests/cytoscape-validation.spec.ts` - Update cola → fcose references
- `e2e-studio/tests/graph-export.spec.ts` - Add clipboard functionality test

## Security Impact

| Severity | Before | After | Change |
|----------|--------|-------|--------|
| MEDIUM | 4 | 1 | -3 |
| LOW | 6 | 5 | -1 |
| Informational | 27 | ~10 | -17 |

## Testing Plan

### Build
```bash
# Build Java backend and Docker image
mvn clean install -DskipTests -Pdocker
```

### E2E Tests
```bash
cd e2e-studio && npm test
```

### Manual Verification
1. Copy to clipboard in graph export modal
2. Toast notifications (success/error scenarios)
3. Graph layout rendering and animation
4. Graph export formats (JSON, PNG, JPEG, GraphML)

## Risk Assessment

| Change | Risk | Mitigation |
|--------|------|------------|
| Clipboard API | Low | Well-supported in modern browsers |
| SweetAlert2 Toast | Low | Already using SweetAlert2 in project |
| fcose layout | Medium | Visual testing required, layout may differ slightly |

## Browser Support

- Native Clipboard API: Chrome 66+, Firefox 63+, Safari 13.1+, Edge 79+
- Requires HTTPS or localhost (already required for ArcadeDB Studio)

## References

- [Clipboard API - MDN](https://developer.mozilla.org/en-US/docs/Web/API/Clipboard_API)
- [SweetAlert2 Toast](https://sweetalert2.github.io/)
- [cytoscape-fcose](https://github.com/iVis-at-Bilkent/cytoscape.js-fcose)
- [Cytoscape.js Layouts Guide](https://blog.js.cytoscape.org/2020/05/11/layouts/)
