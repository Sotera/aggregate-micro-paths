# Dummy config pile to test relative imports
# in hive UDF functions.
# call_udf.py calls "hive -e python my_udf.py",
# and my_udf.py imports config and calls config.math.

import math
