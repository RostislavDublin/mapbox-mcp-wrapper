# mapbox-mcp-wrapper

`mapbox-mcp-wrapper` is a thin, upstream-friendly wrapper around `@mapbox/mcp-server`. Its purpose is to keep the upstream Mapbox MCP tool surface usable in agentic and headless workflows by enforcing non-interactive geocoding and search behavior while preserving upstream compatibility everywhere else.

## Current Implementation

This repository is not a fork of `@mapbox/mcp-server`. It is a separate wrapper package that is meant to be the only MCP server registered in the host. The wrapper starts the upstream Mapbox MCP server as an internal child process and proxies the upstream tool surface over stdio.

The current implementation is intentionally narrow:

- One visible MCP server: `mapbox-mcp-wrapper`
- One upstream dependency: `@mapbox/mcp-server`
- One targeted behavior change: remove `initialize.params.capabilities.elicitation` before forwarding the initialize request upstream
- One fallback policy: if an upstream `elicitation/create` request still appears, immediately return `{ "action": "decline" }`

That combination forces the upstream `search_and_geocode_tool` to use its existing non-interactive fallback path for ambiguous `2..10` result searches instead of surfacing host-rendered selection UI.

## Runtime Model

- The host should register only this wrapper, not both the wrapper and the upstream Mapbox server.
- The wrapper launches the upstream Mapbox MCP process internally from the local dependency when available and falls back to `npx -y @mapbox/mcp-server` only if needed.
- The wrapper preserves upstream tool names and schemas by proxying requests and responses instead of reimplementing individual tools.
- The wrapper uses the current MCP SDK stdio transport format: newline-delimited JSON messages, not legacy `Content-Length` framing.

## Validation Status

The current implementation has been validated locally with the official MCP SDK client:

- SDK `connect()` succeeds through the wrapper
- SDK `listTools()` returns the upstream Mapbox tool surface through the wrapper
- Ambiguous `search_and_geocode_tool` calls complete non-interactively through the wrapper
- The wrapper can expose opt-in debug tracing with `MAPBOX_MCP_WRAPPER_DEBUG=1`

## Problem Statement

The upstream Mapbox MCP geocoding and search flow may trigger MCP elicitation when a query returns 2 to 10 plausible matches. In interactive clients such as VS Code with Copilot, that elicitation can become host-rendered selection UI. That behavior is acceptable for human-in-the-loop use, but it is a bad fit for agentic flows, automation, and headless environments where tools must complete without asking the host to render an interactive chooser. This wrapper exists to remove that dependency on host UI and enforce deterministic, non-interactive behavior for the problematic path.

## Design Goals

- Remain a thin wrapper around the upstream `@mapbox/mcp-server` package.
- Preserve the full upstream tool surface, tool names, argument shapes, and response shapes as much as possible.
- Keep default behavior identical to upstream except for the targeted customization needed to suppress interactive elicitation in the problematic geocoding/search path.
- Provide non-interactive geocoding and search behavior that is safe for agentic execution.
- Stay upstream-friendly: prefer extension seams over forks, keep the maintenance delta small, and make rebasing or upgrading to newer upstream releases cheap.

## Non-goals

- Reimplement Mapbox APIs, authentication, request logic, or MCP transport behavior from scratch.
- Introduce product features unrelated to the elicitation problem, such as caching, persistence, analytics, or UI.
- Change upstream tool semantics broadly or add opinionated ranking logic beyond what is required to suppress user-facing elicitation.
- Maintain a permanent hard fork of the upstream server.
- Add repository extras such as CI, licensing changes, or packaging variations unless they are later needed for actual delivery.

## Architecture

### Recommended implementation approach

Implement this project as a Node.js and TypeScript package that depends directly on `@mapbox/mcp-server`. The wrapper should construct or load the upstream server, proxy or delegate all upstream tools, and replace only the current customization target: `SearchAndGeocodeTool`.

The preferred implementation shape is:

- Reuse upstream server initialization and tool registration as much as possible.
- Preserve every upstream tool unchanged unless a tool participates directly in the problematic elicitation path.
- Wrap `SearchAndGeocodeTool` so that it delegates to upstream logic for request parsing, API calls, ranking, formatting, and result mapping.
- Intervene only at the decision point where the upstream implementation would attempt MCP elicitation for ambiguous results.
- Force a deterministic non-interactive path instead of host-rendered selection UI.

Two implementation strategies are acceptable:

- Intercept or override the elicitation branch and force the same fallback behavior that upstream already uses when elicitation is declined, unsupported, or errors.
- If a clean upstream-compatible capability or control seam exists, use that seam to disable interactive elicitation without forking the rest of the logic.

The wrapper should prefer the second strategy when it is clean, stable, and version-resilient. If no such seam is exposed, the wrapper should use the first strategy and keep the override as narrow as possible.

### Current customization target

The current and only planned customization target is the upstream `SearchAndGeocodeTool`. The project should assume that other upstream tools are passed through unchanged until a concrete, separately justified issue appears.

### Behavioral basis from upstream

The design assumption for this wrapper is based on current upstream behavior: the ambiguous-result elicitation path is triggered for 2 to 10 search results, and the upstream implementation falls back to returning all results when elicitation is declined, unsupported, or errors. This wrapper should intentionally force that non-interactive fallback behavior before any host UI prompt can appear.

## Behavioral Specification

This section is normative for the future implementation.

### General rules

- The wrapper must never trigger host UI prompts for geocoding or search resolution.
- The wrapper must never require MCP elicitation support from the host.
- For all tools other than the targeted geocoding/search customization, behavior should match upstream as closely as possible.
- When ambiguity exists, the wrapper should preserve upstream candidate ordering and return a deterministic non-interactive result.
- The preferred deterministic result for the ambiguous 2 to 10 range is to return the full candidate set in the same order the upstream fallback path would return it.

### Expected behavior by result count

#### 0 results

- Return a successful non-interactive response indicating no matches.
- Preserve the upstream no-result response shape where possible.
- Include wrapper metadata stating that no ambiguity existed and zero candidates were found.

#### 1 result

- Return the single result without any prompt.
- Preserve the upstream single-result response shape where possible.
- Include wrapper metadata stating that no ambiguity existed and one candidate was found.

#### 2 to 10 results

- Never invoke MCP elicitation and never rely on host capabilities for disambiguation.
- Return the same candidate list that upstream would return when elicitation is declined, unsupported, or fails.
- Preserve candidate ordering exactly as produced by upstream.
- Include wrapper metadata that marks the response as ambiguous, states the candidate count, and records that interactive elicitation was intentionally suppressed.
- Do not choose a single winning result unless upstream later exposes an explicit non-interactive selection policy that is cleaner and still upstream-compatible.

#### More than 10 results

- Preserve the normal upstream non-interactive behavior.
- Do not introduce extra filtering, prompting, or ranking changes.
- Include wrapper metadata with the candidate count, but treat this as a normal upstream path rather than a suppressed elicitation path.

### Required metadata for ambiguous responses

When the result set is in the ambiguous 2 to 10 range, the wrapper should add a non-breaking metadata object without removing or renaming upstream fields. The recommended field name is `wrapper_metadata` unless the implementation discovers a cleaner upstream-compatible extension slot.

Recommended metadata shape:

```json
{
  "wrapper_metadata": {
    "non_interactive": true,
    "elicitation_suppressed": true,
    "ambiguity": "ambiguous",
    "candidate_count": 4,
    "selection_mode": "return_all_candidates",
    "reason": "upstream_2_to_10_result_elicitation_range"
  }
}
```

Additional rules:

- Metadata must be additive only.
- Metadata must not break consumers that already understand upstream responses.
- If the upstream response format has no structured metadata slot, the wrapper may append a short text note while keeping structured results unchanged, but additive structured metadata is preferred.

## Upstream Evidence

The upstream project is [mapbox/mcp-server](https://github.com/mapbox/mcp-server). The geocoding disambiguation behavior relevant to this wrapper was introduced in [mapbox/mcp-server PR #98](https://github.com/mapbox/mcp-server/pull/98) and the associated [mapbox/mcp-server commit 2eea0e3](https://github.com/mapbox/mcp-server/commit/2eea0e3), which added an elicitation-based ambiguous geocoding flow. A future implementation should also inspect the capability-related control path around [mapbox/mcp-server commit 303bf3d](https://github.com/mapbox/mcp-server/commit/303bf3d) before overriding behavior directly, because that path may expose a cleaner upstream-compatible seam for disabling interactive elicitation.

The implementation should treat the following observations as the starting evidence base to verify against the pinned upstream version during coding:

- The upstream repository is the source of truth for server structure and exported tool surface.
- The 2 to 10 result range is the relevant elicitation window for ambiguous geocoding/search.
- The upstream fallback behavior on elicitation decline, unsupported capability, or elicitation error is to return all results.
- This wrapper should preserve that fallback semantics and make it unconditional for the ambiguous path.

## Proposed Repository Layout

```text
.
├── README.md
├── package.json
├── tsconfig.json
├── src/
│   ├── index.ts
│   ├── server.ts
│   ├── upstream/
│   │   ├── loadUpstreamServer.ts
│   │   └── toolRegistry.ts
│   ├── tools/
│   │   ├── NonInteractiveSearchAndGeocodeTool.ts
│   │   └── passthroughTools.ts
│   └── metadata/
│       └── wrapperMetadata.ts
└── test/
    ├── contract/
    │   └── upstreamSurface.spec.ts
    ├── geocoding/
    │   └── nonInteractiveSearch.spec.ts
    └── integration/
        └── mcpServer.spec.ts
```

Repository layout intent:

- `src/upstream/` contains the minimal adapter code for loading and registering the upstream server and tools.
- `src/tools/NonInteractiveSearchAndGeocodeTool.ts` contains the only behavior override.
- `src/tools/passthroughTools.ts` exists only if the implementation needs an explicit registry proxy.
- `src/metadata/wrapperMetadata.ts` centralizes additive metadata generation so the behavior stays consistent and testable.
- `test/contract/` proves that the wrapper remains aligned with upstream tool names and basic schemas.

## Implementation Plan

### Milestone 1: Bootstrap the wrapper

- Create a minimal TypeScript package targeting a current supported Node.js runtime.
- Add `@mapbox/mcp-server` as a direct dependency.
- Stand up a wrapper server that can expose the upstream MCP tool surface.

### Milestone 2: Preserve upstream surface

- Enumerate upstream tools from the pinned dependency version.
- Ensure the wrapper re-exports or proxies every upstream tool.
- Add contract tests that fail when an upstream tool disappears unexpectedly from the wrapper or when the wrapper mutates schemas unnecessarily.

### Milestone 3: Implement the non-interactive geocoding override

- Identify the exact upstream elicitation branch inside or around `SearchAndGeocodeTool`.
- Implement the narrowest possible override that forces the non-interactive fallback path for 2 to 10 results.
- Keep all non-targeted behavior delegated to upstream logic.

### Milestone 4: Add ambiguity metadata

- Add additive wrapper metadata for ambiguous results.
- Verify that metadata does not remove or rename upstream fields.
- Keep the metadata shape stable and documented.

### Milestone 5: Harden compatibility and release readiness

- Verify compatibility with a pinned upstream version.
- Document any unavoidable divergence from upstream.
- Prepare the package for publication only after behavior and compatibility tests pass.

## Testing Strategy

The future implementation must include automated tests that prove the wrapper does not depend on elicitation-driven UI and that upstream passthrough behavior remains intact.

Required test categories:

- Unit tests for the result-count decision logic covering 0, 1, 2 to 10, and more than 10 results.
- Tests that assert no MCP elicitation call is made when ambiguous geocoding returns 2 to 10 results.
- Tests that simulate hosts with no elicitation capability and verify identical wrapper behavior.
- Tests that simulate elicitation decline or error and verify the wrapper still returns the deterministic candidate list.
- Contract tests that compare the wrapper tool surface to the upstream tool surface and ensure non-targeted tools are passed through unchanged.
- Integration tests for the wrapper MCP server transport to prove the wrapper can be used by an MCP client without any host-side selection UI.
- Response-shape tests verifying that wrapper metadata is additive and does not break upstream fields.

Specific assertions that must exist:

- Ambiguous search responses never block waiting for a host prompt.
- Ambiguous search responses return all upstream candidates in upstream order.
- The wrapper continues to expose the same upstream tools and argument contracts except for additive metadata on the targeted path.
- Non-geocoding tools behave as pure passthroughs.

## Open Questions and Future Evolution

- Does the pinned upstream version expose a stable capability hook that cleanly disables elicitation without overriding tool internals?
- What is the cleanest additive metadata slot that preserves compatibility with existing MCP clients?
- Should a future version allow an explicit opt-in flag to restore interactive elicitation for human-operated clients, or should the package remain strictly non-interactive by design?
- How should upstream version pinning and compatibility policy be expressed: exact version, narrow range, or periodic revalidation against upstream main?
- If upstream eventually adds a first-class non-interactive mode, should this wrapper collapse into a minimal configuration layer instead of retaining any override code?

## How to use this repo in a future implementation session

Treat this README as the source-of-truth implementation spec. A future coding agent should begin by pinning and inspecting the target upstream `@mapbox/mcp-server` version, confirming the exported tool surface, locating the `SearchAndGeocodeTool` elicitation branch, and then implementing only the minimum wrapper code required to preserve upstream compatibility while forcing non-interactive behavior for the ambiguous 2 to 10 result path. Avoid adding unrelated features unless a concrete implementation blocker requires them.