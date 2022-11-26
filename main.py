# importing findspark to add pyspark to sys.path at runtime
# source: https://towardsdatascience.com/how-to-use-pyspark-on-your-computer-9c7180075617
import findspark
findspark.init()

# step #1 - output the nc file to csv
# for now, just testing with a singular beach in 2007, we may want to experiment with combining multiple nc's, etc
# source: https://stackoverflow.com/questions/44359869/convert-netcdf-file-to-csv-or-text-using-python
# uncomment the below lines as necessary, otherwise use test files (output.csv and small output.csv)
'''
import xarray as xr

nc = xr.open_dataset('https://sccoos.org/thredds/dodsC/autoss/newport_pier-2007.nc')
nc.precip.to_dataframe().to_csv('output.csv')
'''

# step #2 - import csv module we created earlier
# put headers separately into one list and data in another list
# source: https://www.geeksforgeeks.org/python-read-csv-column-into-list-without-header/
import csv

with open('output.csv', newline='') as file:
    reader = csv.reader(file, delimiter=',')

    # store column names into a list here
    columns = next(reader)

    # store all ocean algae data into a list here
    data = []
    for row in reader:
        data.append(row[:])

# uncomment these as necessary
# show contents of csv file
print("content:", data)

# show contents of columns
print("headers:", columns)

# step #3 - start a spark session from pyspark.sql module
# source: https://www.geeksforgeeks.org/find-minimum-maximum-and-average-value-of-pyspark-dataframe-column/
from pyspark.sql import SparkSession

# create spark session using the 'oceanspark' name
spark = SparkSession.builder.appName('oceanspark').getOrCreate()

# creating a dataframe from the data we grabbed from the csvs in step #2
dataframe = spark.createDataFrame(data, columns)

# display dataframe
dataframe.show()

# TEST: find average of temperature column
dataframe.agg({'temperature': 'avg'}).show()