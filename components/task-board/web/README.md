# React + TypeScript + Vite

This template provides a minimal setup to get React working in Vite with HMR and some Oxlint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Oxc](https://oxc.rs)
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the Oxlint configuration

If you are developing a production application, we recommend enabling type-aware lint rules by installing `oxlint-tsgolint` and editing `.oxlintrc.json`:

```json
{
  "$schema": "./node_modules/oxlint/configuration_schema.json",
  "plugins": ["react", "typescript", "oxc"],
  "options": {
    "typeAware": true
  },
  "rules": {
    "react/rules-of-hooks": "error",
    "react/only-export-components": ["warn", { "allowConstantExport": true }]
  }
}
```

See the [Oxlint rules documentation](https://oxc.rs/docs/guide/usage/linter/rules) for the full list of rules and categories.


## Native Fleet view

Open **Native Fleet** in the Board header (route `/native`), or the native
sessions/hosts link on the home dashboard. This consumes the shared Board's
`/api/fleet/hosts` and paginated `/api/fleet/dispatches`, not a browser's own
localhost runtime. Host capacity/freshness/revision, independent session IDs,
agent/task links, retry/error details, per-session workspaces and observed
runtime phase/step/progress are visible together. Observations refresh on Fleet
SSE events and every five seconds while visible; a failed read retains the last
observation with an explicit error and automatic retry. Old model progress alone
is never labeled a dead worker.

The view supports URL filters `agent`, `host`, `session`, `attention=1` and
`page`. Proxy subpaths are derived from the existing Board base URI. No lifecycle
writes or automatic replays are performed by the view.

```sh
npm ci
npm run test:native
npm run build
# Optional real-browser integration against the built assets and loopback fixtures:
CHROMIUM_BIN=/path/to/chromium-or-headless-shell npm run test:native:browser
```

The browser check is dependency-free CDP on Node 22; it checks real rendering,
proxy subpaths, same-identity sessions, retry/unknown tools, stale hosts, escaped
text, filters, API outage recovery and a 420px viewport. Set
`NATIVE_UI_SCREENSHOT=/path/to/evidence.png` to retain a screenshot. This fixture
check supplements, but does not replace, opening the deployed shared Board.
