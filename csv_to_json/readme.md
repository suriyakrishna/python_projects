## CSV to JSON Converter

csv_to_json.py script defined in scripts folder is used to convert a CSV file to JSON file.

#### Script Help
```bash
$ python scripts/csv_to_json.py --help
```

```text
usage: csv_to_json.py [-h] -t TYPE [-c CONFFILEPATH] [-i INPUTFILEPATH]
                      [-o OUTPUTFILEPATH] [-d DELIMITER] [-l LOGFILEPATH]
                      [-n COLUMNNAMES]

CSV to JSON Converter

optional arguments:
  -h, --help            show this help message and exit
  -t TYPE, --type TYPE  Configuration type JSON/ARGS
  -c CONFFILEPATH, --confFilePath CONFFILEPATH
                        JSON Configuration File path
  -i INPUTFILEPATH, --inputFilePath INPUTFILEPATH
                        Input CSV File Path
  -o OUTPUTFILEPATH, --outputFilePath OUTPUTFILEPATH
                        Output path to store JSON File
  -d DELIMITER, --delimiter DELIMITER
                        CSV File delimiter
  -l LOGFILEPATH, --logFilePath LOGFILEPATH
                        Log File Path
  -n COLUMNNAMES, --columnNames COLUMNNAMES
                        List of Column Names - For files without header
```

#### Script Invocation
##### Invoking Using JSON Configuration file

JSON Config file can be defined as shown below. delimiter, column_names and log_file_path are optional arguments.
```json
{
  "input_file" : "C:\\Users\\Kishan\\Desktop\\python_projects\\csv_to_json\\input\\employee.csv",
  "output_file" : "C:\\Users\\Kishan\\Desktop\\python_projects\\csv_to_json\\output\\employee.json",
  "delimiter" : ",",
  "column_names" : "name,department,manager,salary",
  "log_file_path" : "C:\\Users\\Kishan\\Desktop\\python_projects\\csv_to_json\\logs"
}
```

```bash
$ python scripts/csv_to_json.py -t json -c conf_files/employee_conf.json
```

##### Invoking Using Command Line options

Python script can be invoked as shown below. delimiter, column_names and log_file_path are optional arguments. In my case my input file is `~` delimited and it doesn't contains column names. So I invoked with delimiter and column_names parameter

```bash
$ python scripts/csv_to_json.py -t ARGS -i input/student.csv -o output/student.json -d "~" -n "name,age,class" -l logs/
```


