# Use Database


```python

from database import Db

>>> my_db = Db()
>>> my_db.insert({'hello': 'boy'})
>>> my_db.update({'hello': 'boy'}, {'hello': 'goodbye'})
{'hello', 'goodbye'}
>>> my_db.find({})
{'hello', 'goodbye'}

```

# Launch the Tests

```bash

git clone git@github.com:elijahbal/database.git
cd database
pip install --user pytest
pytest -v .
```
