/**
 * @deprecated This breaks when there are more than one user profiles
 * @todo Support identifying the default profile
 */
export const getUserSettingsJsonPath = (userId?: string): string => {
  /** @deprecated This breaks when there are more than one user profiles. See commented out `if` statement below. */
  const appliedUserId = !userId ? "*" : userId;

  /**
   * 1. Parse
   * @note Replace the ternary above with this. */
  if (!userId) {
    // 1. Look at vscodium's "profiles" directory:
    const vscodiumProfilesDirectory = "$HOME/Library/Application Support/VSCodium/User/profiles";
    // ... logic to: "find all directories within vscodiumProfilesDirectory" ...
    const vscodiumProfileDirectoryLabels: string[] = [];

    // 2. (Candidate logic, poorly reasoned-through first-draft) Recursively walk through each dir and find the right filesread through perform some sort of
    //  2. Recursively walk through each dir
    /** 2. Iter thru each profile directory
     * 1. Recursively walk through each dir. Within each dir:
     * 2. (@todo define `filepathOfFileWhichShowsIfDefault`) Parse contents of `filepathOfFileWhichShowsIfDefault` for target line(s)
     */
    const getFilepathOfFileWhichShowsIfDefault = (profileDirectoryLabel: string): string =>
      `${vscodiumProfilesDirectory}/${profileDirectoryLabel}/path/to/file.xyz`;

    vscodiumProfileDirectoryLabels.forEach((profileDirectoryLabel) => {
      const filepath = getFilepathOfFileWhichShowsIfDefault(profileDirectoryLabel);
    });

    // ... logic to: ...
  }

  return `$HOME/Library/Application Support/VSCodium/User/profiles/${appliedUserId}/settings.json`;
};

// "$HOME/Library/Application Support/VSCodium/User/profiles/*/settings.json";
export const userSettingsJsonPath: string = getUserSettingsJsonPath();

export const getRepoSettingsJsonPath: (repoPath: string) => string = (repoPath) => {
  // 1. Expand shell variables, if present
  // @todo ... logic to: Expand shell variables/shortcuts (like `~/`, `$HOME`, etc.) ...
  const appliedRepoPath = !!repoPath;

  // 1. Validate path
  // @todo ... logic to: validate `repoPath` - 1. Does it exist ...
  const doesRepoPathExist = !!appliedRepoPath;
  if (!doesRepoPathExist) {
    throw Error(`Repo path: "${repoPath}" does not exist`);
  }

  return `${repoPath}/.vscode/settings.json`;
};
