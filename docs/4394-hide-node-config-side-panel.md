# Issue #4394: Hide Node Config on Side Panel

## Summary

Added a show/hide toggle button for the Appearance section in the graph record editor side panel.

## Problem

The graph node side panel always showed the Appearance (node config) section, and there was no way to collapse it. After the initial configuration, this section occupies half the panel space unnecessarily.

## Solution

Added a chevron toggle button next to the "Appearance" section header that:

- Collapses/expands the appearance settings body with a single click
- Persists the collapsed/expanded state in `localStorage` under key `graphAppearanceCollapsed`
- Restores the correct state on subsequent node selections (the state is read every time `renderGraphAppearanceSection` is called)
- Uses a chevron-up icon when expanded and chevron-down when collapsed

## Files Changed

- `studio/src/main/resources/static/js/studio-record-editor.js`
  - `renderGraphAppearanceSection()`: wrapped appearance body in `#geAppearanceBody` div; added toggle button with class `record-editor-appearance-toggle`; reads `graphAppearanceCollapsed` from localStorage to set initial state
  - `toggleGraphAppearanceSection()`: new function that toggles body visibility, swaps chevron icon, and persists state to localStorage

## Testing

This is a pure frontend (JavaScript/HTML) change with no backend impact. Verification steps:

1. Open ArcadeDB Studio
2. Run a graph query that returns vertices
3. Click a node - the side panel opens showing Properties, Actions, and Appearance sections
4. Click the chevron button next to "Appearance" - the section collapses
5. Click again - the section expands
6. Close and reopen the panel (click another node) - the state is restored from localStorage
7. Reload the page - the state is still preserved
