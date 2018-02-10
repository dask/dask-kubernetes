import os

config = {'-'.join(k.split('_')[1:]).lower(): v
          for k, v in os.environ.items()
          if k.startswith('DASKERNETES_')}
