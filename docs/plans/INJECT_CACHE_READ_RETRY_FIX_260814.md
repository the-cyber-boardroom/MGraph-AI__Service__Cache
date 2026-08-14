# Inject cache read retry fix — Cache service impact

**Date:** 2026-08-14  
**Status:** Cross-repository implementation notice  
**Branch:** `investigate/inject-stub-s3-miss-260814`

## Canonical specification

The full fix is owned and specified in:

`MGraph-AI__Service__mitmproxy/docs/plans/INJECT_CACHE_READ_RETRY_FIX_260814.md`

The fix prevents the mitmproxy inject service from confusing a temporary
Cache/S3 read failure with a genuinely missing publisher filter.

## Cache service responsibility

No Cache production-code change is currently expected.

The Cache service must preserve the distinction it already exposes:

- found string content → HTTP 200;
- found empty string → HTTP 200 with an empty body;
- true missing string → HTTP 404 with an empty body;
- backend/S3 failure → exception or error response, never a false HTTP 404.

The mitmproxy service will retry one failed read exactly once and create
`EMPTY_STUB` only after a confirmed miss.

## Required verification

Retain or add focused tests proving:

- `Routes__Data__Retrieve.handle_string_result()` returns HTTP 404 only when
  `found` is false;
- an empty but found string remains HTTP 200;
- an exception from `Storage_FS__S3` is not converted to `found=False` or HTTP
  404;
- no Cache code silently changes an uncertain backend outcome into a confirmed
  absence.

## Compatibility constraints

- Do not change the existing string route paths.
- Do not change true-miss HTTP 404 semantics.
- Do not add retry logic here for this fix; the single retry belongs to the
  mitmproxy inject read policy.
- Keep existing `Type_Safe` request/response schemas and one-class-per-file
  conventions.
- If verification finds that a backend exception is currently converted into
  `found=False`, stop and revise the cross-repository fix before deployment.

## Release impact

Documentation/tests alone require no version bump. If production Cache code
does need modification after verification, use this repository's independent
SemVer source of truth when releasing; do not couple its version to mitmproxy
or Cache Client.
