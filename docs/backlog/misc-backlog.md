1. Create script `update-user-settings-json-from-codium` - Looks up a user profile (apply "default" profile if none provided, requires figuring out the logic to auto-derive "default" profile), gets current `settings.json`, replaces current repo with that.
2. Create script `update-user-settings-json-to-codium` - Applies the current settings.json in this repo and overwrites the provided user's (use default if none provided) current settings.json
3. Create script `update-workspace-settings-json-from-codium` - Same as above, but with workspace `settings.json`
4. Create script `update-workspace-settings-json-to-codium` - Same as above, but with workspace `settings.json`
5. Create script `update-repo-settings-json-to-codium` - Same as above, but with repo `settings.json`
6. Create script `update-repo-settings-json-to-codium` - Same as above, but with repo `settings.json`
