<!-- markdownlint-disable MD022 MD024 -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-04-12

### Changed

- add an optional `Blueprints` checkbox to the Constructs tab player-scoped search, with persisted settings and restored selection kind
- extend construct suggestions with a kind marker so blueprint-only rows can be shown as `[BP]` entries and handled separately from live constructs
- load selected `[BP]` suggestions through the blueprint import path instead of trying to open them as live construct snapshots
- add a PostgreSQL blueprint-by-creator lookup for the scoped Constructs dropdown and update the scoped status text to indicate when blueprints are included
- keep restored construct selection stable by matching on both id and suggestion kind
- exclude blueprint suggestions whose names contain `<SNAPSHOT>` from the Constructs dropdown
- sort the merged dropdown in two blocks: live constructs first, blueprint-only entries second, with name/id ordering inside each block
- show database-loaded blueprint details in the Construct Browser summary when a blueprint is opened from the Constructs tab
- Improve databank tools, backup safety, and detail pane UX
- clarify construct filtering and status text in the Constructs tab
- show preferred databank element names instead of generic Databank [id] labels
- add lower detail toolbars for Databank, LUA blocks, and HTML/RS with save, refresh, and pretty-print actions
- fix detail pane splitter/layout behavior so lower toolbars stay in their own row and previews keep focus/selection across refresh
- add databank clear support with confirmation, live reload, and DB-side write handling
- extend the backup system with content-kind aware entries, databank backup/restore flows, and a shared backup dialog mode for databanks
- require backup creation before Lua Save to DB and before databank clear/restore writes
- create databank safety backups from the live locked DB value instead of the cached UI snapshot
- improve the backup dialog with side-by-side comparison, line diff output, and stale-selection guards
- decode databank backup content with the same blob-decoder path used by the live UI and block unsafe legacy corrupted databank backups from restore
- harden generated save/export/backup file names so app-created names stay ASCII-only and avoid PowerShell-sensitive characters such as brackets
- bump the app version from 1.1.0 to 1.2.0

- Added Readme

## [1.0.0] - 2026-02-16

- Initial public release

[1.2.0]: https://github.com/tobitege/myDUWorkbench/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/tobitege/myDUWorkbench/compare/v1.0.0...v1.1.0
