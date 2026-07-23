---
name: Release group
about: Coordinate multiple PRs that must reach production together
title: "release-group: "
labels: release-group
assignees: ""
---

Release-Group-Anchor: #0
Release-Group-Members: #0, #0

<!--
Replace every #0. The anchor must be a listed member and its PR must
Depends-On every other member. Keep this issue open until production succeeds.
Only repository owners, members, or collaborators may own a release-group issue.
Every member PR must copy this complete anchor/member snapshot into its own
.github/release-coordination/contracts/pr-<PR_NUMBER>.json file. Once merged,
contract files are append-only and must never be edited, renamed, or deleted.
-->
