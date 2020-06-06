
from pyspark.sql import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import datetime
import os
import sys

class data_frame_data_validation:

    def __init__(self, df1, df2, primary_keys, list_of_columns=None, num_records_to_validate=5):
        if(isinstance(df1, DataFrame) and isinstance(df1, DataFrame)):
            self.df1 = df1
            self.df2 = df2
            self.intersecting_columns = list(set(df1.columns).intersection(df2.columns))
            if(len(self.intersecting_columns) == 0):
                print("Provided Data Frame doesn't have same schema. Exiting..")
                sys.exit(1)
            self.num_records_to_validate = list(range(1,num_records_to_validate+1))
            if(isinstance(primary_keys, str)):
                self.primary_keys = list(map(lambda x: x.strip(), primary_keys.split(",")))
            else:
                self.primary_keys = primary_keys
            if(list_of_columns != None):
                if(isinstance(list_of_columns, str)):
                    self.list_of_columns = list(map(lambda x: x.strip(), list_of_columns.split(",")))
                else:
                    self.list_of_columns = list_of_columns
            else:
                self.list_of_columns = df1.columns
        else:
            print("Parameter type violation. Exiting")
            sys.exit(1)

    def create_window(self):
        window = Window.orderBy(self.primary_keys)
        return window

    def data_preparation(self, df, no_of_rows_to_validate):
        w = self.create_window()
        output = df.withColumn("row_num", row_number().over(w)).filter(col("row_num").isin(no_of_rows_to_validate)).orderBy(col("row_num")).collect()
        return list(map(lambda x: x.asDict(), output))

    def do_validation(self, output_file_path=None):
        source_list_dict = self.data_preparation(self.df1, self.num_records_to_validate)
        dest_list_dict = self.data_preparation(self.df2, self.num_records_to_validate)
        a = []
        b = []
        r = []
        for i in range(len(self.num_records_to_validate)):
            A = []
            B = []
            R = []
            for column in self.list_of_columns:
                df1_value = str(source_list_dict[i][column])
                df2_value = str(dest_list_dict[i][column])
                A.append(df1_value)
                B.append(df2_value)
                R.append(str(df1_value == df2_value))
            a.append("~".join(A))
            b.append("~".join(B))
            r.append("~".join(R))
        c = "~".join(self.list_of_columns)
        a = "\n".join(a)
        b = "\n".join(b)
        r = "\n".join(r)

        print("SOURCE DATA\n")
        print(c)
        print(a)
        print("\nDESTINATION DATA\n")
        print(c)
        print(b)
        print("\nRESULT\n")
        print(c)
        print(r)
        
        if(output_file_path != None):
            current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
            output_file = os.path.join(output_file_path, "data_validation_output_{}.csv".format(current_timestamp))
            with open(output_file, 'w') as write_output:
                write_output.write("SOURCE DATA\n")
                write_output.writelines(c+"\n")
                write_output.writelines(a+"\n")
                write_output.write("\nDESTINATION DATA\n")
                write_output.writelines(c+"\n")
                write_output.writelines(b+"\n")
                write_output.write("\nRESULT\n")
                write_output.writelines(c+"\n")
                write_output.writelines(r+"\n")