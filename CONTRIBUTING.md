# Contributing Guidelines

## Getting Started
- Clone the repository and create a feature branch off `main`.
- Use Python 3.10+ and install dependencies in editable mode:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -e .[dev]
  ```
- Copy `.env.example` to `.env` (if provided) and populate any secrets locally.

## Development Workflow
1. Update or add unit tests alongside code changes.
2. Run `pytest` before opening a pull request.
3. Keep commits focused and write descriptive commit messages.
4. Update documentation (README, data contracts, changelog) when behaviour changes.
5. Submit a pull request that:
   - Describes the change, motivation, and testing performed.
   - Links relevant issues or tickets.

## Code Style
- Follow idiomatic Python 3.10+ style. Use `ruff` or `black` if configured.
- Prefer small, composable functions with clear logging.
- Avoid hard-coding secrets; pull them from environment variables or secret stores.

## Review Expectations
- Be receptive to feedback and iterate quickly.
- Apply suggestions or explain alternative approaches.
- Ensure CI passes before requesting final review.

Thank you for helping make Pipeline(ML*) better!
