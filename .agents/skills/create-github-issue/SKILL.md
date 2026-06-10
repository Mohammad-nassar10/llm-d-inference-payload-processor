---
name: create-github-issue
description: Create GitHub issues using the gh CLI. Use when the user wants to create a new issue, report a bug, request a feature, or create a task in GitHub. Trigger keywords - create issue, new issue, file bug, report bug, feature request, github issue.
---

# Create GitHub Issue

Create issues on GitHub using the `gh` CLI. Issues must conform to the project's issue templates.

## Prerequisites

The `gh` CLI must be authenticated (`gh auth status`).

## Issue Templates

This project uses markdown issue templates located in `.github/ISSUE_TEMPLATE/`. The templates automatically apply labels:
- Bug reports: `kind/bug, needs-triage`
- Feature requests: `kind/feature, needs-triage`
- Blank issues: `needs-triage`

### Bug Reports

Use the Bug Report template for reporting bugs. The template includes labels `kind/bug, needs-triage` automatically.

```bash
gh issue create \
  --title "<concise description of the bug>" \
  --body "$(cat <<'EOF'
**What happened**:

<Describe what actually happened>

**What you expected to happen**:

<Describe what you expected to happen>

**How to reproduce it (as minimally and precisely as possible)**:

1. <step>
2. <step>
3. <step>

**Anything else we need to know?**:

<Additional context, agent diagnostic output, or investigation findings>

**Environment**:
- Kubernetes version (use `kubectl version`):
- llm-d-inference-payload-processor version (use `git describe --tags --dirty --always` if you built from source, or specify the tag if you used a tagged version or image):
- Cloud provider or hardware configuration:
- Install tools:
- Others:
EOF
)"
```

### Feature Requests

Use the Feature Request template for suggesting enhancements. The template includes labels `kind/feature, needs-triage` automatically.

```bash
gh issue create \
  --title "<concise description of the feature>" \
  --body "$(cat <<'EOF'
**What would you like to be added**:

<Describe the feature or enhancement you'd like to see>

**Why is this needed**:

<Explain the problem this solves and why it matters>
EOF
)"
```

### Blank Issues

For issues that don't fit bug/feature templates, use a blank issue. The template includes label `needs-triage` automatically.

```bash
gh issue create \
  --title "<descriptive title>" \
  --body "$(cat <<'EOF'
<Your issue description here>
EOF
)"
```

**Note**: Blank issues are disabled by default in this repository (`.github/ISSUE_TEMPLATE/config.yml`). Users must select either Bug Report or Feature Request templates.

## Useful Options

| Option              | Description                        |
| ------------------- | ---------------------------------- |
| `--title, -t`       | Issue title (required)             |
| `--body, -b`        | Issue description                  |
| `--label, -l`       | Add label (can use multiple times) |
| `--milestone, -m`   | Add to milestone                   |
| `--project, -p`     | Add to project                     |
| `--web`             | Open in browser after creation     |

## After Creating

The command outputs the issue URL and number.

**Display the URL using markdown link syntax** so it's easily clickable:

```
Created issue [#123](https://github.com/OWNER/REPO/issues/123)
```

Use the issue number to:

- Reference in commits: `git commit -m "Fix validation error (fixes #123)"`
- Create a branch following project convention: `<issue-number>-<description>/<username>`
