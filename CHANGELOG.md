# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# [Unreleased]

## Added

- Similarity: `numeric` and `geo` scorers for a column, which compare by absolute difference and
  by great-circle distance in kilometres, with `within` and an optional `at_most` in place of
  `above` and `cutoff`. See `examples/radiocarbon/README.md`.
- Examples 08 to 10 and a radiocarbon sample, merging the AIDA and p3k14c databases.

## Fixed

- pandas 3 compatibility:
  - a blank value in a similarity merge's reference column no longer crashes the merge;
  - a blank title no longer crashes the legacy title-set evaluation;
  - replace `inplace=True` calls that pandas has deprecated.
- Similarity: a record with a blank reference value is now a no-match. It used to be paired
  with an arbitrary record, and marked exact when no other configured column could tell them apart.

# [1.0.1] - 2024-04-24

## Fixed

- Similarity:
  - make columns case insensitive;
  - assert no duplicate columns in yaml.


# [1.0.0] - 2024-03-15

- Initial release. See the README for the features available in this release.

[unreleased]: https://github.com/sci2zero/TeslaSCIToolkit/compare/1.0.1...HEAD
[1.0.1]: https://github.com/sci2zero/TeslaSCIToolkit//releases/tag/1.0.1
[1.0.0]: https://github.com/sci2zero/TeslaSCIToolkit//releases/tag/1.0.0