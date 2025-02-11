# tesci

## An interactive toolkit for merging data from multiple citation databases

[![PyPI](https://img.shields.io/pypi/v/tesci.svg)](https://pypi.org/project/tesci/)
[![License-1]( https://img.shields.io/badge/License-Apache-blue.svg)](https://img.shields.io/badge/License-Apache-blue.svg)
[![License-2]( https://img.shields.io/badge/License-MIT-green.svg)](https://img.shields.io/badge/License-MIT-green.svg)
[![Python-Versions](https://img.shields.io/pypi/pyversions/tesci.svg)](https://img.shields.io/pypi/pyversions/tesci.svg)

## Overview

`TeslaSCIToolkit` (abbrev. `tesci`) is a scientific mapping tool that comes with the following features:

 1. Merging data from multiple citation databases
 2. Restricting access to sensitive columns in data sources with aggregations
 3. Exporting transformed data into other repositories
 4. CI/CD integration, currently GitHub Actions

For examples and use-cases, see [examples](examples/) directory.

## Quickstart

### Aggregating data from a single database source

To create an aggregation of [`simple.csv`](examples/simple/simple.csv) based on average `salary` and `age`.

#### 1. Interactive approach

```properties
tesci start -d simple.csv -o exported.csv​
tesci aggregate avg -c salary -a avg_salary​
tesci aggregate avg -c age -a avg_age​
tesci apply​
```

#### 2. Configuration approach

```yml
aggregate:​
  - alias: avg_salary​
    column: salary​
    function: avg​
  - alias: avg_age​
    column: age​
    function: avg​
data:​
  dest: exported.csv​
  src: simple.csv
```

The result is a transformation from `simple.csv` to `exported.csv`:

| | | |
|--|--|--|
|<table> <tr><th>id</th><th>name</th><th>email</th><th>phone-number</th><th>age</th><th>salary</th></tr><tr><td>1</td><td>John Doe</td><td>john@mail.com</td><td>1234567890</td><td>33</td><td>100000</td></tr><tr><td>2</td><td>Jane Doe</td><td>jane@mail.com</td><td>0987654321</td><td>44</td><td>200000</td></tr><tr><td>3</td><td>John Smith</td><td>smith@mail.com</td><td>1234509876</td><td>55</td><td>300000</td></tr><tr><td>4</td><td>Jane Williams</td><td>jwilliams@mail.com</td><td>1234509876</td><td>31</td><td>98000</td></tr><tr><td>5</td><td>Jack Miller</td><td></td><td>1234509876</td><td>33</td><td>79000</td></tr> </table>|&rarr;|<table> <tr><th>avg_salary</th><th>avg_age</th></tr><tr><td>155400.0</td><td>39.2</td></tr> </table>|

### Merging data from multiple citation databases

After retrieving data sources from citation databases of your choice, place the databases in a directory of your choice.
Then, specify the configuration used for merging. An example of a configuration is [here](examples/bibliometric-study/config.yml).

After specifying your configuration choices, merge can then by run with:

`tesci similarity merge --src PATH --src PATH --dest DIR`

where PATH and DIR refer to relative or absolute filesystem paths and directories.

## Citing `tesci`

If you find `tesci` useful in your research, please support our work by citing our paper.

Nikolić, D., Ivanović, D., & Ivanović, L. (2024).
An open-source tool for merging data from multiple citation databases.\
Scientometrics, 1-23.
https://doi.org/10.1007/s11192-024-05076-2

## License

Licensed under either of Apache License, Version 2.0 or MIT license.
