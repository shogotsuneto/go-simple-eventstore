### ⚠️ Breaking Changes
- **EventStore.Append signature change**: Now returns `(int64, error)` instead of `error` to provide latest version after append
- **Empty append behavior**: Empty appends now always return version `0` regardless of current stream state

### ✨ Features
- **Append operation now returns latest version**: Event producers can know the current version after successful append
- **In-place event updates**: Original events passed to `Append()` are updated with assigned versions, IDs, and timestamps
