# Communication style

- Follow ASD-STE100

# Pull requests

## CHANGELOG

- **Do not** edit `CHANGELOG.md` or `ext-arrow/CHANGELOG.md`. Parallel pull requests that edit these files conflict.
- Write the CHANGELOG entry in the pull request description.
- Write one short sentence if possible. Tell what changes for the user.
- Put the entry under a section name that `CHANGELOG.md` uses: `Added`, `Changed`, `Deprecated`, `Removed` or `Fixed`.
- End the entry with a link to the pull request, not to the issue. If you do not know the pull request number yet, open the pull request first, then update the description.
- If the change is in `ext-arrow/`, say that the entry is for `ext-arrow/CHANGELOG.md`.
- If the change has no effect on users (for example tests, CI or refactoring), write "No CHANGELOG entry".

Example:

~~~md
### Fixed

* Fixed handling of JSON type hints starting with `SKIP`. ([#485])

[#485]: https://github.com/ClickHouse/clickhouse-rs/pull/485
~~~
