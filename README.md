To start dev db: run `docker-compose up -d`

Requires a .env file in the root directory (or environment variables defined.) A sample can be seen.

# Usage:

after installing as a library (pip install), the runner can be invoked as such:

```
from courier import process

def lambda_handler(event, context):
    process(event, context)
```

# build

To create a new build: Update the version in setup.py run
`python setup.py bdist_wheel`
a new build will appear in dist/
