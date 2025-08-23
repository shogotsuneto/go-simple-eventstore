# Release Pipeline

This repository includes an automated release pipeline that helps create releases and maintain the CHANGELOG.md file.

## How to Create a Release

1. **Ensure all changes are in the CHANGELOG.md**
   - Add your changes to the `[Unreleased]` section of `CHANGELOG.md`
   - Follow the existing format with categories: ⚠️ Breaking Changes, ✨ Features, 🐛 Bug Fixes, 🔧 Improvements

2. **Trigger the Release Workflow**
   - Go to the "Actions" tab in GitHub
   - Select the "Create Release" workflow
   - Click "Run workflow" 
   - Enter the version in format `vX.Y.Z` (e.g., `v0.0.6`)
   - Click "Run workflow"

3. **Review the Results**
   - The workflow will create a **draft release** with content from the unreleased section
   - The workflow will create a **pull request** to update CHANGELOG.md

4. **Complete the Release**
   - Review and merge the CHANGELOG pull request
   - Review and publish the draft release

## What the Pipeline Does

### Draft Release Creation
- Extracts content from the `[Unreleased]` section of CHANGELOG.md
- Creates a draft GitHub release with that content as release notes
- Uses the specified version as the release tag and title

### CHANGELOG Update PR
- Moves content from `[Unreleased]` to a new version section with current date
- Adds a new empty `[Unreleased]` section for future changes
- Creates a pull request with these changes against the `develop` branch

## Version Format

Versions must follow semantic versioning format: `vX.Y.Z`

Examples:
- ✅ `v0.0.6`
- ✅ `v1.2.3` 
- ✅ `v10.20.30`
- ❌ `0.0.6` (missing 'v' prefix)
- ❌ `v1.2` (missing patch version)
- ❌ `v1.2.3.4` (too many version parts)

## Workflow Permissions

The release workflow requires:
- `contents: write` - To create releases and tags
- `pull-requests: write` - To create pull requests

These permissions are automatically available to the workflow when using `GITHUB_TOKEN`.

## Error Handling

The workflow will fail if:
- Version format is invalid
- Version already exists as a git tag
- No unreleased content is found in CHANGELOG.md
- GitHub API calls fail

## Manual Steps After Automation

1. **Review the Pull Request**: Check that the CHANGELOG.md changes look correct
2. **Merge the Pull Request**: This updates the main branch with the new version information  
3. **Review the Draft Release**: Verify the release notes and metadata
4. **Publish the Release**: This makes the release public and creates the git tag

The automation handles the tedious parts while keeping human oversight for the final publishing steps.