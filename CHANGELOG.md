# Changelog

This file documents **breaking changes only** in the go-simple-eventstore library. 

Breaking changes are modifications that require existing users to update their code to continue working with the new version. This includes changes to public APIs, function signatures, default behaviors, and configuration options.

For a complete list of all changes including new features, bug fixes, and improvements, please refer to the [GitHub Releases](https://github.com/shogotsuneto/go-simple-eventstore/releases) page.

## [v0.0.5] - 2025-08-20

### Breaking Changes

#### PostgreSQL Configuration API Changes

**Impact:** Existing PostgreSQL users must update their code to continue working.

##### `InitSchema` Function Signature Change

The `InitSchema` function now requires an additional `useClientTimestamps` parameter:

```go
// Before v0.0.5
InitSchema(db, tableName)

// v0.0.5 and later
InitSchema(db, tableName, useClientTimestamps)
```

**Migration:** Add the third parameter to control timestamp generation:
- Use `false` for database-generated timestamps (recommended default)
- Use `true` for application-generated timestamps (legacy behavior)

##### `Config` Struct Field Addition

The PostgreSQL `Config` struct now includes a new field:

```go
type Config struct {
    ConnectionString string
    TableName string
    UseClientGeneratedTimestamps bool  // New field
}
```

**Migration:** Update your config initialization:

```go
// Before v0.0.5
config := postgres.Config{
    ConnectionString: "...",
    TableName: "events",
}

// v0.0.5 and later
config := postgres.Config{
    ConnectionString: "...",
    TableName: "events",
    UseClientGeneratedTimestamps: false, // Use database timestamps (recommended)
}
```

##### Default Timestamp Behavior Change

- **Before v0.0.5:** All timestamps were generated in the application layer
- **v0.0.5 and later:** Default is database-generated timestamps using `DEFAULT CURRENT_TIMESTAMP`

This change improves consistency and reduces clock skew issues in distributed environments.

**Migration:** If you need the old behavior (application-generated timestamps), set `UseClientGeneratedTimestamps: true` in your config.