#### Pyspark - Data Validation Framework

The purpose of this project is to validate data in two dataframes dynamically based on their schema and the primary keys.

This project consists a module `pyspark_data_validation_framework.py` which has python class `data_frame_data_validation` that is used for data validation.


##### About `data_frame_data_validation` class

`data_frame_data_validation` consist of constructor which can accepts 5 parameters.
- First Parameter is the source DataFrame which is of type DataFrame, mandatory parameter.
- Second Parameter is the destination DataFrame which is of type DataFrame, mandatory parameter.
- Third Parameter is the parameter for primary_keys which can be of type list of strings or string of column names seperated by comma, mandatory parameter.
- Fourth Parameter is the parameter for list_of_columns which need to be validated this list should always need to include primary keys, optional.
- Fifth Parameter is the parameter for num_records_to_validate by default it will be 5, optional parameter and value should not exceed the number of records in source or destination dataframes.

##### About `do_validation` method under `data_frame_data_validation` class

This method is invoked in order to do the data validation. When this method is invoked it will do the validations and print the output in the console. If we want output in a file we can provide the output path to this method and it will write the output to the file also.

##### Usage:
```python
# importing data validation module

import sys
sys.path.append("../scripts")
from pyspark_data_validation_framework import data_frame_data_validation

# Creating Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark - Data Validation").getOrCreate()

# Creating Two DataFrames which has same schema
import os
source_file_path = os.path.join("../inputs", "employee_source.csv")
destination_file_path = os.path.join("../inputs", "employee_destination.csv")
header = "true"
infer_schema = "true"
source_df = spark.read.option("infer", infer_schema).option("header", header).csv(source_file_path)
destination_df = spark.read.option("infer", infer_schema).option("header", header).csv(destination_file_path)

data_validation = data_frame_data_validation(source_df, destination_df, primary_keys=["name"], num_records_to_validate=6)

data_validation.do_validation("../outputs")
```


##### Sample Output before Text To Columns:

![Raw Output](images/excel_output_before_delimiting.png?raw=true "Raw Output") 

##### Sample Output after Text To Columns:

For the better understanding of the results we can use Text To Columns in Microsoft Excel and delimiter of the file is `~`

![Text to Column Output](images/excel_output_after_delimiting.png?raw=true "Text to Column Output")