# Contributing Guide

## Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd python-template
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --dev
   ```

4. **Install pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

## Git Workflow (Feature Branch)

This project uses a **feature branch workflow** with `main` as the production branch that all changes are merged into via pull requests.

### Making Changes

1. **Start with the latest main**
   ```bash
   git checkout main
   git pull origin main
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b your-branch-name
   ```

3. **Make your changes on the feature branch**
   - Edit files as needed
   - Add new features or fix bugs
   - Follow the coding standards (enforced by pre-commit hooks)

4. **Test your changes**
   ```bash
   uv run pytest
   uv run pre-commit run --all-files
   ```

5. **Commit your changes**
   ```bash
   git add <your-files>
   git commit -m "descriptive commit message"
   ```

6. **Push your feature branch**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Open a pull request** targeting the `main` branch

### Commit Message Guidelines

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")

**Examples:**
```
Add user authentication system

Implement JWT-based authentication with login/logout endpoints.
Includes password hashing and session management.

Fixes #123
```

### Pull Request Process

1. **Open a pull request** from your feature branch targeting `main`
   - Use the provided PR template
   - Provide a clear description of your changes
   - Link any related issues

2. **Address review feedback**
   - Make changes on your feature branch
   - Push additional commits to update the PR
   ```bash
   git add <your-files>
   git commit -m "Address review feedback"
   git push origin your-branch-name
   ```

3. **Merge requirements**
   - All CI checks must pass
   - At least one approving review required
   - No merge conflicts with main

## Code Quality Standards

### Pre-commit Hooks
This project uses pre-commit hooks to ensure code quality. The following checks run automatically:

- **Ruff**: Code formatting and linting
- **pyright**: Type checking
- **YAML/TOML validation**
- **Trailing whitespace removal**
- Several others for general file checks

### Running Quality Checks Manually

```bash
# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Testing

- Write tests for all new functionality
- Maintain test coverage above 80%
- Run tests locally before committing:
  ```bash
  uv run pytest
  ```

## Project Structure

```
python-template/
├── python_template/          # Main package directory
│   ├── __init__.py
│   └── ...
├── tests/                    # Test directory
│   ├── __init__.py
│   └── ...
├── .github/                  # GitHub templates and workflows
│   ├── workflows/
│   └── pull_request_template.md
├── .pre-commit-config.yaml   # Pre-commit configuration
├── pyproject.toml           # Project configuration and dependencies
├── README.md
└── CONTRIBUTING.md
```

## Troubleshooting

### Pre-commit Hook Failures
If pre-commit hooks fail:
1. Review the error messages
2. Fix the issues
3. Re-add and commit your changes

### uv Issues
- Update uv: `uv self update`
- Clear cache: `uv cache clean`
- Reinstall dependencies: `uv sync --reinstall --dev`
