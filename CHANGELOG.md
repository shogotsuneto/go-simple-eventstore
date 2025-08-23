# Changelog

This file documents all user-relevant changes in the go-simple-eventstore library, including breaking changes, new features, improvements, and bug fixes.

## [Unreleased]

### ⚠️ Breaking Changes
- **EventStore.Append signature change**: Now returns `(int64, error)` instead of `error` to provide latest version after append
- **Empty append behavior**: Empty appends now always return version `0` regardless of current stream state

### ✨ Features
- **Append operation now returns latest version**: Event producers can know the current version after successful append
- **In-place event updates**: Original events passed to `Append()` are updated with assigned versions, IDs, and timestamps

## [v0.0.5] - 2025-08-20

### ⚠️ Breaking Changes
- **PostgreSQL `InitSchema` signature change**: Added `useClientTimestamps` parameter
- **PostgreSQL `Config` struct change**: Added `UseClientGeneratedTimestamps` field
- **Default timestamp behavior change**: Now uses database-generated timestamps by default

### ✨ Features
- **PostgreSQL database-generated timestamps**: New option for improved consistency and reduced clock skew
- **Configurable timestamp generation**: Choose between database or client-generated timestamps

## [v0.0.4] - 2025-08-05

### ⚠️ Breaking Changes
- **EventStore interface field renames**: Updated field names in the EventStore interface

### ✨ Features
- **Descending load logic**: Added support for loading events in descending order

## [v0.0.3] - 2025-08-04

### ⚠️ Breaking Changes
- **PostgreSQL default table name removed**: No longer provides a default table name

### ✨ Features
- **Per-table consumer interface**: Added EventConsumer interface and implementations for table-specific event consumption

## [v0.0.2] - 2025-07-29

### ✨ Features
- **PostgreSQL configurable table names**: Added support for custom table names with explicit configuration API

## [v0.0.1] - 2025-07-28

### ✨ Features
- **Initial release**: Basic EventStore interface with in-memory and PostgreSQL implementations