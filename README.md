# Redquery

Python [Redash](https://redash.io/) API Client

## Installation

```sh
$ pip install git+https://github.com/MasamichiIdeue/redquery.git
```


## Usage

```python
from redquery import Client

c = Client('https://redash.example.host', api_key, data_sourece_id)
c.query('query').rows
```
