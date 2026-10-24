# ButchrD

ButchrD is an isolated daemon package. It does not modify or replace the
existing `brooswit-factory/butchr` repository.

## Build and verify

Requires Node.js 20 or newer. A clean checkout is reproducible with:

```sh
npm ci
npm run build
npm test
```

## Dependency consumption

Use pinned Git commits or workspace dependencies for Morbin and Morbr until
package publication is separately approved. This project has no runtime
dependency on legacy Butchr.
