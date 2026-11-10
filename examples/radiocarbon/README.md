# Radiocarbon sample

A few dates from two archaeological radiocarbon databases, for running examples 08 to 10:

- `aida-sample.csv`: 14 rows from [AIDA](https://github.com/apalmisano82/AIDA), the Archive of
  Italian radiocarbon DAtes, v5 (Palmisano, Bevan, Kabelindde, Roberts & Shennan 2022,
  doi:10.5281/zenodo.6427045), licensed [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
  Columns are a subset of the original.
- `p3k14c-italy-sample.csv`: 11 Italian rows from [p3k14c](https://github.com/people3k/p3k14c)
  2022.06 (Bird et al. 2022, *Scientific Data* 9, 27), whose data is
  [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). Columns are a subset of the original.

The rows were chosen to show each kind of disagreement between the two databases: an identical
record, a coordinate disagreement (`A-4472`, `GrA-19820`, `R-2702`), an age disagreement (`R-98`
appears twice in AIDA, at 3075 and 3975 BP), two lab codes that differ only in case but belong to
different dates (`LTL-201A`, `LTL-201a`), and codes written differently for the same measurement
(`I-15466`/`L-15466`, `Lyon-4043`/`LY-4043`). One AIDA row has no lab code and should never be paired.

Run an example from a folder holding a `.tesci/config.yml` copied from it:

```
mkdir -p .tesci && cp examples/09-config-radiocarbon-field-conflicts.yml .tesci/config.yml
tesci similarity merge --src examples/radiocarbon/aida-sample.csv --src examples/radiocarbon/p3k14c-italy-sample.csv
```

`config-final-pairs.csv` records which AIDA row (`first_index`) was paired with which p3k14c row
(`second_index`) and the bucket each pair landed in.

## Distance scorers

Examples 09 and 10 compare ages and positions with a distance rather than by text similarity:

```yaml
- from_: "Age"
  into_: "CRA"
  similarity:
    scorer: numeric   # absolute difference
    within: 0         # 0 is exact; up to within is suggested
    at_most: 100      # up to at_most is potential, beyond it no match; omit for no limit
- from_: "Lat"
  into_: "Latitude"
  similarity:
    scorer: geo       # great-circle distance in kilometres
    longitude:
      from_: "Long"
      into_: "Longitude"
    within: 25
```

The default scorer, `qratio`, compares text and keeps using `above` and `cutoff`. The reference
column must use it.
