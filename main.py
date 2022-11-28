# importing findspark to add pyspark to sys.path at runtime
# source: https://towardsdatascience.com/how-to-use-pyspark-on-your-computer-9c7180075617
import time

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
print("read data from file start")
startTime = time.monotonic()
with open('output.csv', newline='') as file:
    reader = csv.reader(file, delimiter=',')

    # store column names into a list here
    columns = next(reader)

    # store all ocean algae data into a list here
    data = []
    for row in reader:
        data.append(row[:])

endTime = time.monotonic()
print("read data from file end, in ", (endTime - startTime), " s")

# make note of number of rows in array "data": rows = # of measurements -1 (for headers
entries = len(data) - 1
print("The number of entries in data is ", entries)

# uncomment these as necessary
# show contents of csv file
#print("content:", data)

# show contents of columns
#print("headers:", columns)

# if not printing whole contents of array "data" use this line to show output.csv has finished being read
print("finished reading output.csv")

# step #2.1 - follow the steps in this guide to install hadoop (and the correct version of Java) on your machine
# source: https://medium.com/analytics-vidhya/hadoop-how-to-install-in-5-steps-in-windows-10-61b0e67342f8
# step #2.2 - follow the steps in this guide to install pySpark on your machine
# source: https://gongster.medium.com/how-to-use-pyspark-in-pycharm-ide-2fd8997b1cdd
# step # 2.9 - use time measurement capability per https://docs.python.org/3/library/time.html
# step #3 - start a spark session from pyspark.sql module
# source: https://www.geeksforgeeks.org/find-minimum-maximum-and-average-value-of-pyspark-dataframe-column/
from pyspark.sql import SparkSession

# create spark session using the 'oceanspark' name
print("create spark session")
startTime = time.monotonic()
spark = SparkSession.builder.appName('oceanspark').getOrCreate()
endTime = time.monotonic()
print("spark session made, in ", (endTime - startTime), " s")

# count down from number A to (number B +1),
#  rows to read (maxRows) from array "data" becomes larger with each loop
#  ex. maxRows = rows in data / number ... rows / 3, rows / 2 , rows / 1, stop
for number in range(4, 0, -1):
    # create a max number of rows to read
    maxRows = round(entries / number)

    # creating a dataframe from the data we grabbed from the csvs in step #2
    print("create dataframe")
    startTime = time.monotonic()
    dataframe = spark.createDataFrame(data[0:maxRows], columns)
    endTime = time.monotonic()
    print("dataframe created, in ", (endTime - startTime), " s")

    # display dataframe
    # dataframe.show()

    # TEST: find average of temperature column
    print("average dataframe")
    startTime = time.monotonic()
    dataframe.agg({'temperature': 'avg'}).show()
    endTime = time.monotonic()
    print("dataframe avg complete, in ", (endTime - startTime), " s")