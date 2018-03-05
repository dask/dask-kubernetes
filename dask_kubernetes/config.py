"""
We capture all environment variables named

    DASKERNETES_FOO_BAR=value

And turn them into key-value pairs in a global ``config`` dictionary.
We change the keys to remove ``DASKERNETES_``, replace underscores with
hyphens, and lower-case the entire thing.  This results in a Python dictionary
like the following:

    >>> config
    {'foo-bar': 'value'}
"""

import os

config = {'-'.join(k.split('_')[1:]).lower(): v
          for k, v in os.environ.items()
          if k.startswith('DASKERNETES_')}
