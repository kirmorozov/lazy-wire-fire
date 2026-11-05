# Team

The agents that work on this repository, and what each one owns.

| Role | Job | Agent | Owns |
|---|---|---|---|
| `lwf-super` | supervises the other agents and decides the order of delivery | Claude Code | no repository file |
| `lwf-git-keeper` | merges pull requests that are marked ready and green, CI/CD and releases | opencode2 | `docs/COMMITTING.md`; commits the doc changes |
| `lwf-doc-keeper` | keeps `README.md` and `./docs` in line with the code | opencode2 | `README.md` and every file in `./docs` except `docs/COMMITTING.md` (`TEAM.md` included) |
| `lwf-worker-<task>` | one job each; commits on its own branch, pushes it and runs its own pull request: draft, CI green, `gh pr ready` | opencode2 or Claude Haiku | the files its task names, in its own worktree when they may conflict |
