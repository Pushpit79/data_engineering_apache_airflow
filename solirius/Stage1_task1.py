# Task 1 - Import the input dataset, convert the data types, then save as a parquet file.
# schema.json file should be used to load the dataset


import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
folderdir = Path("c:/Alison/Solirius/solirius-data-engineering-coding-test-master/solution")

importdatafromparquet = pq.ParquetFile("c:/Alison/Solirius/solirius-data-engineering-coding-test-master/solution/parquet/task1output.parquet")


print(importdatafromparquet)



