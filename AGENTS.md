Before any session read .env and set the environment variables.

BEFORE you start:
1. Check skills and tools that can be used for the task.
2. Always use skills and tools when available.

## Changing the code

Always use the following when making code or configuration changes:
1. make fmt for formatting
2. make lint for linting 
3. make test for running all the unit tests

## Skills

Agent skills live in `.agents/skills/`. Your harness can discover and load them natively — do not rely on this file for a full inventory.

### Available Skills

| Skill | Description | Trigger Keywords | Documentation |
|-------|-------------|------------------|---------------|
| **create-github-issue** | Create GitHub issues using the gh CLI | create issue, new issue, file bug, report bug, feature request, github issue | [SKILL.md](.agents/skills/create-github-issue/SKILL.md) |
| **create-github-pr** | Create GitHub pull requests using the gh CLI | create PR, pull request, new PR, submit for review, code review | [SKILL.md](.agents/skills/create-github-pr/SKILL.md) |
| **review-github-pr** | Review GitHub PRs by summarizing diffs and design decisions | review PR, review pull request, summarize PR, summarize diff, code review, review branch | [SKILL.md](.agents/skills/review-github-pr/SKILL.md) |

