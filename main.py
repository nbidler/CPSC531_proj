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
# open the list of databases and extract the list of locally stored CSV files
databaseFilenames = []
with open('urls_sorted.txt', newline='') as file:
    for url in file:
        dataName = url[url.rfind('/') + 1:url.rfind('.')]
        databaseFilenames.append(dataName + '.csv')

print(databaseFilenames)
# step #2 - import csv module we created earlier
# put headers separately into one list and data in another list
# source: https://www.geeksforgeeks.org/python-read-csv-column-into-list-without-header/
import csv

# the too-large array to hold all data
# store all ocean algae data into a list here
data = []

for filename in databaseFilenames:
    # debug line for timing reading input file
    print("read data from file start")
    startTime = time.monotonic()

    with open(filename, newline='') as file:
        reader = csv.reader(file, delimiter=',')

        # store column names into a list here
        columns = next(reader)

        # store data from file into list, which pySpark can read
        for row in reader:
            data.append(row[:])

    # end of reading input file
    endTime = time.monotonic()

    print("read ", len(data), " lines of data from file, in ", (endTime - startTime), " s")

    # for purposes of testing, keep the "maximum" size small
    if len(data) > 10000:
        break

# if not printing whole contents of array "data" use this line to show output.csv has finished being read
print("finished reading .csv files")

# make note of number of rows in array "data": rows = # of measurements -1 (for headers
entries = len(data)
print("The number of entries in data is ", entries)

# uncomment these as necessary
# show contents of csv file
#print("content:", data)

# show contents of columns
#print("headers:", columns)

# step #2.1 - follow the steps in this guide to install hadoop (and the correct version of Java) on your machine
# source: https://medium.com/analytics-vidhya/hadoop-how-to-install-in-5-steps-in-windows-10-61b0e67342f8
# step #2.2 - follow the steps in this guide to install pySpark on your machine
# source: https://gongster.medium.com/how-to-use-pyspark-in-pycharm-ide-2fd8997b1cdd
# step # 2.9 - use time measurement capability per https://docs.python.org/3/library/time.html
# step #3 - start a spark session from pyspark.sql module
# source: https://www.geeksforgeeks.org/find-minimum-maximum-and-average-value-of-pyspark-dataframe-column/
from pyspark.sql import SparkSession

# create spark session using the 'oceanspark' name
#print("create spark session")
#startTime = time.monotonic()
spark = SparkSession.builder.appName('oceanspark').getOrCreate()
#endTime = time.monotonic()
#print("spark session made, in ", (endTime - startTime), " s")

# Step #4 - create and act on the dataframe repeatedly,
#   each time using a larger portion of the dataset up to the dataset's actual size
# count down from number A to (number B +1),
#  rows to read (maxRows) from array "data" becomes larger with each loop
# change this variable to change the size otaf the dataset being used
# ex. lines to be read each time will be = dataset * (1 / dataPortion)

# lists all fractions up to a given denominator
fractions = []
target = 5

for denominator in range(target, 0, -1):
    for numerator in range(1, target):
        if (numerator < denominator):
            if (denominator % numerator != 0) or (numerator == 1):
                fractions.append([numerator, denominator])

# finally do full data set, 1/1
fractions.append([1, 1])
# create storage list of 5-tuples
timeMeasures = []

for number in fractions:
    print("this dataframe contains ", number[0], "/", number[1], " of the total data")
    # create a max number of rows to read
    maxRows = round(entries * (number[0] / number[1]))
    print("rows to read: ", maxRows)
    # creating a dataframe from the data we grabbed from the csvs in step #2
    #print("create dataframe")
    startTime = time.monotonic()
    dataframe = spark.createDataFrame(data[0:maxRows], columns)
    endTime = time.monotonic()
    wholeDFtime = endTime - startTime
    #print("whole dataframe created, in ", wholeDFtime, " s")
    numPartitions = dataframe.rdd.getNumPartitions()
    #print("Maximum partitions ", numPartitions)

    # Step #5 - limiting number of partitions that the operation can run on
    # source: https://towardsdatascience.com/how-to-efficiently-re-partition-spark-dataframes-c036e8261418
    for activePartitions in range(1, numPartitions+1):
        # over-write dataframe with itself, limited to activePartitions number of partitions
        reducedDF = dataframe.coalesce(activePartitions)
        print("this dataframe contains ", activePartitions, " active Partitions")
        #print("dataframe with reduced partitions created in ", wholeDFtime, " s")
        # TEST: find average of temperature column
        #print("average dataframe with ", activePartitions, " partitions")
        startTime = time.monotonic_ns()
        reducedDF.agg({'temperature':'avg', 'conductivity':'avg', 'salinity':'avg', 'chlorophyll':'avg'}).collect()#.show()
        #reducedDF.agg('temperature', 'conductivity', 'salinity', 'chlorophyll')
        endTime = time.monotonic_ns()
        avgDFtime = endTime - startTime
        #print("dataframe avg complete, in ", avgDFtime, " s")
        # store DF size, num partitions, time taken for wholeDF, reducedDF, avg
        timeMeasures.append([maxRows, activePartitions, wholeDFtime, avgDFtime])

    if maxRows > 100000:#number[1]<4:
        break

# we now have all the time data in one place

# X axis displays number of lines read
xAxis = []
# Y axis displays time, but must have multiple arrays, DF creation and AVG operation
yDF = []
yAVG = []

# we now have all the time data in one place

# create number of y measurements equal to numPartitions
# number of partitions for data point = array index (1 part at index 1, 2 at 2, etc)
for amtPartitions in range(numPartitions+1):
    yDF.append([])
    yAVG.append([])

curPos = 0
endPos = len(timeMeasures)
for curPos in range(endPos):
    #print(type(timeMeasures[curPos][0]), " ", timeMeasures[curPos][0])
    #print("rows read: ", timeMeasures[curPos][0], " partitions : ", timeMeasures[curPos][1], " creating wholeDF: ", timeMeasures[curPos][2], " time to avg reduced DF: ", timeMeasures[curPos][3])
    # keep track of what amt of partition was being used
    parts = (curPos+1) % numPartitions
    if parts == 0:
        parts = 12
    # print("curPos ", curPos, " partitions ", parts)
    # only measure the xAxis every time it changes
    if parts == 1:
        #print(timeMeasures[curPos])
        xAxis.append(timeMeasures[curPos][0])
    yDF[parts].append(timeMeasures[curPos][2])
    #print(timeMeasures[curPos])
    yAVG[parts].append(timeMeasures[curPos][3])
    #print(timeMeasures[curPos])

# print("xAxis ", xAxis)
# print("yDF ")
# for entry in yDF:
#     print(entry)
# print("yAVG ")
# for entry in yAVG:
#     print(entry)

# Step #5 - having gathered the data, visualize it for ease of understanding
# source: https://matplotlib.org/stable/tutorials/introductory/pyplot.html
# source: https://stackoverflow.com/questions/4971269/how-to-pick-a-new-color-for-each-plotted-line-within-a-figure-in-matplotlib

import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import numpy as np

colors = cm.rainbow(np.linspace(0, 1, len(yDF)))
plt.figure(1)
dataSlices = len(yDF) -1
#print("xAxis ", len(xAxis), " yDF ", len(yDF), " yAVG ", len(yAVG), " colors ", len(colors))

#print("yDF")
for index in range(dataSlices):
    # print("index ", index)
    # print("xAxis ", xAxis)
    # print(" yDF ", yDF[index+1])
    # print(" yAVG ", yAVG[index+1])
    # print(" colors ", colors[index])
    plt.plot(xAxis, yDF[index+1], c=colors[index], label= (index+1, 'partitions'))
    #print(index, " ", colors[index])

plt.xlabel('Lines Read')
plt.ylabel('Time (seconds)')
plt.title("time to create dataframe")
plt.savefig("DFgraph.png")
plt.legend()
#plt.show()

plt.figure(2)

#print("yAVG")
for index in range(dataSlices):
    plt.plot(xAxis, yAVG[index+1], c=colors[index], label= (index+1, 'partitions'))
    #print(index, " ", colors[index])

plt.xlabel('Lines Read')
plt.ylabel('Time (seconds)')
plt.title("time to average dataframe")
plt.savefig("AVGgraph.png")
plt.legend()
#plt.show()