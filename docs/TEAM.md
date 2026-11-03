# Team

The agents that work on this repository, and what each one owns.

| Role | Job | Agent | Owns |
|---|---|---|---|
| `lwf-super` | supervises the other agents and decides the order of delivery | Claude Code | no repository file |
| `lwf-git-keeper` | commits, pushes, pull requests, CI/CD and releases | opencode2 | `docs/COMMITTING.md`; commits every other change once `lwf-super` says it is ready |
| `lwf-doc-keeper` | keeps `README.md` and `./docs` in line with the code | opencode2 | `README.md` and every file in `./docs` except `docs/COMMITTING.md` (`TEAM.md` included) |
| `lwf-worker-<task>` | one job each | opencode2 or Claude Haiku | the files its task names, in its own worktree when they may conflict |
