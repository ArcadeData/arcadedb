# Studio CLAUDE.md

This file provides guidance to Claude Code when working with the ArcadeDB Studio frontend.

## Overview

ArcadeDB Studio is the web-based administration interface for ArcadeDB. It is a **traditional multi-page SPA** (not React/Vue/Angular) built with jQuery 4.0 + Bootstrap 5.3. All JavaScript modules use global functions and variables attached to the window scope. There is no module bundler for application code - only vendor libraries are processed by Webpack.

## Technology Stack

| Category | Library | Version |
|----------|---------|---------|
| DOM/AJAX | jQuery | 4.0 |
| UI Framework | Bootstrap | 5.3.8 |
| Code Editor | CodeMirror | 5.x (modes: SQL, Cypher, JavaScript) |
| Graph Visualization | Cytoscape.js | 3.33+ (plugins: fcose, cxtmenu, graphml, node-html-label) |
| Data Tables | DataTables | 2.3+ (extensions: buttons, responsive, select) |
| Charts | ApexCharts | 5.4+ |
| Notifications | SweetAlert2 | 11.x |
| Icons | FontAwesome Free | 7.2 |
| Export | JSZip (Excel), pdfmake (PDF) |
| API Docs | Swagger UI | 5.x |

**Constraint**: Only jQuery and Bootstrap 5 are allowed as core frameworks. Third-party libraries must be Apache 2.0 compatible (see root CLAUDE.md for allowed licenses).

## Build System

### npm + Webpack (vendor bundling only)

Webpack is used **solely** to copy vendor libraries from `node_modules/` into `src/main/resources/static/dist/`. It does NOT bundle, transpile, or process the Studio application JS files. The application JS files are loaded directly via `<script>` tags in `index.html`.

```
npm install          # Install dependencies
npm run build        # Webpack production build (copies vendors to dist/)
npm run dev          # Webpack watch mode for development
npm run clean        # Remove dist/ and node_modules/
npm run security-audit  # Comprehensive security audit
```

### Maven Integration

The `pom.xml` uses `frontend-maven-plugin` to run npm during Maven build:
- `npm install` runs during dependency resolution phase
- `npm run build` runs during `generate-resources` phase
- Node.js v18.19.0 is auto-installed by the plugin
- Build output is packaged into the JAR at `static/`

**Full build from project root**: `mvn clean install` (includes Studio build)
**Studio-only build**: `cd studio && npm run build`

### Build Output

```
src/main/resources/static/dist/    # Webpack output (vendor JS/CSS/fonts)
  js/         # ~30+ vendor JS files (minified)
  css/        # ~10 vendor CSS files
  webfonts/   # FontAwesome fonts
```

This folder is committed to git and deployed inside the JAR.

## Directory Structure

```
studio/
  src/main/
    js/
      vendor-libs.js              # Webpack entry point (minimal, just for webpack)
    resources/static/
      index.html                  # Main SPA entry point (loads all tabs)
      query.html                  # Query editor tab (SQL, Cypher, Gremlin, etc.)
      database.html               # Database schema management tab
      server.html                 # Server monitoring & metrics tab
      cluster.html                # Cluster/HA management tab
      api.html                    # Swagger API documentation tab
      resources.html              # Help & links tab
      popup-login.html            # Login modal dialog
      js/                         # Application JavaScript modules
        studio-utils.js           # Global utilities (alerts, cookies, HTML escape, formatting)
        studio-database.js        # Database listing, schema, create/drop/backup (LARGEST ~1350 lines)
        studio-server.js          # Server info, metrics charts, settings (~900 lines)
        studio-cluster.js         # Cluster status & HA monitoring (~240 lines)
        studio-table.js           # DataTables initialization & configuration (~170 lines)
        studio-graph.js           # Graph rendering setup, import/export (~250 lines)
        studio-graph-widget.js    # Cytoscape instance, layout, interactions (~930 lines)
        studio-graph-functions.js # Graph utility functions (~130 lines)
      css/
        studio.css                # Custom Studio styles
      images/                     # Logos, favicon, spinner, social icons
      dist/                       # Generated vendor assets (webpack output)
  scripts/
    copy-swagger-ui.js            # Copies Swagger UI files to static/
    security-audit.sh             # Security audit script
  package.json
  webpack.config.js
  pom.xml
  .nvmrc                          # Node 18.19.0
  .npmrc                          # legacy-peer-deps=true (for jQuery 4.0 compat)
```

## Application Architecture

### HTML Template System

`index.html` is the main entry point. It uses a server-side include directive `${include:filename.html}` to compose tabs at serve time. Each tab is a separate HTML file that gets inlined into the main page.

**Tab structure**: Left vertical sidebar nav (`col-1`) + Right content pane (`col-11`)

Tabs: Query | Database | Server | Cluster | API | Info | Logout

### JavaScript Module Loading Order

All modules are loaded via `<script>` tags in `index.html` (no ES modules, no imports). **Order matters**:

1. Vendor libraries (jQuery, Bootstrap, SweetAlert2, CodeMirror, DataTables, Cytoscape, ApexCharts)
2. Compatibility shims (Bootstrap 4 -> 5 data attribute mapping, Toast init)
3. `studio-utils.js` - Must load first (other modules depend on its globals)
4. `studio-database.js` - Database operations
5. `studio-server.js` - Server monitoring
6. `studio-cluster.js` - Cluster management
7. `studio-table.js` - Table rendering
8. `studio-graph.js` - Graph setup
9. `studio-graph-widget.js` - Graph interactions
10. `studio-graph-functions.js` - Graph utilities

### Global State Variables

All state is in global (window) scope. Key variables:

| Variable | Purpose |
|----------|---------|
| `globalCredentials` | Bearer token for API auth |
| `globalUsername` | Logged-in username |
| `globalResultset` | Current query results (`{ vertices, edges, records }`) |
| `globalCy` | Cytoscape graph instance |
| `globalSelected` | Currently selected graph element |
| `globalGraphSettings` | Graph visualization preferences |
| `globalGraphMaxResult` | Max graph results (default: 1000) |
| `studioCurrentTab` | Active tab name string |
| `editor` | CodeMirror instance |
| `Toast` | SweetAlert2 Toast mixin |

### Session/Auth stored in localStorage:
- `arcadedb-session` - Bearer token
- `arcadedb-username` - Username

### Query Result Flow

1. User writes query in CodeMirror editor
2. Selects language (SQL, Cypher, Gremlin, GraphQL, MongoDB, Redis)
3. Clicks execute -> AJAX POST to server
4. Response parsed into `globalResultset`
5. Rendered in one of three sub-tabs:
   - **Graph** tab: Cytoscape visualization (for vertices/edges)
   - **Table** tab: DataTables rendering (for tabular data)
   - **JSON** tab: Raw JSON display

## Server API Integration

### Authentication

- **Login**: `POST /api/v1/login` with Basic Auth header -> returns `{ token, user }`
- **Logout**: `POST /api/v1/logout`
- All subsequent calls include `Authorization: Bearer {token}` header
- Session persisted in localStorage for auto-login on reload

### Key API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /api/v1/login` | Authenticate |
| `POST /api/v1/logout` | End session |
| `GET /api/v1/databases` | List databases |
| `GET /api/v1/exists/{db}` | Check DB exists |
| `GET /api/v1/query/{db}/{language}/{query}` | Execute query |
| `POST /api/v1/command/{db}` | Execute command |
| `GET /api/v1/server` | Server info & metrics |
| `POST /api/v1/server` | Server management (create/drop DB, backup, etc.) |

### AJAX Pattern

All API calls use jQuery `$.ajax()` with this pattern:
```javascript
$.ajax({
  url: "api/v1/endpoint",
  type: "GET",
  headers: { Authorization: globalCredentials },
  success: function(data) { ... },
  error: function(jqXHR, textStatus, errorThrown) {
    globalNotifyError(jqXHR.responseText);
  }
});
```

## Key Coding Patterns

### Notifications
```javascript
globalNotify("Title", "message", "success");   // Toast notification (top-right)
globalNotify("Title", "message", "danger");     // Error toast
globalAlert("Title", "HTML content", "error");  // Modal alert (SweetAlert2)
globalConfirm("Title", "text", "warning", yesCallback, noCallback);  // Confirm dialog
```

### HTML Escaping
Always use `escapeHtml(value)` when rendering user data into the DOM to prevent XSS.

### localStorage Helpers
```javascript
globalStorageSave("key", value);
globalStorageLoad("key", defaultValue);
```

### Bootstrap Compatibility
The app includes a shim that maps Bootstrap 4 `data-toggle` / `data-dismiss` attributes to Bootstrap 5 `data-bs-toggle` / `data-bs-dismiss`. New code should use the `data-bs-*` prefix directly.

## Testing

**There are currently no frontend tests.** No test framework (Jest, Mocha, etc.) is configured. No `test/` directories exist.

Backend integration with the Studio is tested through the server module's Java test suite (HTTP endpoint tests).

If adding tests, they would need to:
1. Add a test framework to `devDependencies` in `package.json`
2. Add a `test` script to `package.json`
3. Create a test directory structure

## Development Workflow

### Making Changes to Studio JS

1. Edit files directly in `src/main/resources/static/js/`
2. No build step needed for application JS changes
3. Reload the browser to see changes
4. If modifying vendor dependencies: run `npm run build` to update `dist/`

### Adding a New Vendor Library

1. `npm install <package>` in the `studio/` directory
2. Add copy patterns to `webpack.config.js` for the needed files
3. Add `<script>` or `<link>` tags to `index.html`
4. Run `npm run build`
5. Verify license compatibility (Apache 2.0 or allowed list)

### Adding a New Tab/Page

1. Create a new HTML file (e.g., `newtab.html`) with a `<div class="tab-pane">` wrapper
2. Add the `${include:newtab.html}` directive to `index.html`
3. Add a nav item `<li>` in the sidebar
4. Create a corresponding `studio-newtab.js` file
5. Add the `<script>` tag to `index.html` (after dependencies)

### Adding Functionality to an Existing Tab

1. Read the relevant `studio-*.js` file to understand existing patterns
2. Add new functions following the same global function pattern
3. Add UI elements to the corresponding HTML file
4. Use existing utility functions from `studio-utils.js`

## Important Conventions

- **No ES modules**: Everything is global functions. No `import`/`export`.
- **No TypeScript**: Plain JavaScript only.
- **No transpilation**: Code runs directly in the browser as-is.
- **No linting**: No ESLint/Prettier configured.
- **Function naming**: camelCase, often prefixed by module area (e.g., `updateServer()`, `renderGraph()`)
- **jQuery 4.0**: Some jQuery 3.x patterns may not work. `.on()` preferred over deprecated shortcuts.
- **Bootstrap 5**: Use `data-bs-*` attributes, not `data-*` (Bootstrap 4 style).
- **Security**: Always escape user input with `escapeHtml()` before rendering to DOM.
- **No System.out equivalent**: Don't leave `console.log()` debug statements in production code.
