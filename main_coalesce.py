# importing findspark to add pyspark to sys.path at runtime
# source: https://towardsdatascience.com/how-to-use-pyspark-on-your-computer-9c7180075617
import time
import findspark
import csv
from pyspark.sql import SparkSession
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm

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
def getCSVfilenames():
    databaseFilenames = []
    with open('urls_sorted.txt', newline='') as file:
        for url in file:
            dataName = url[url.rfind('/') + 1:url.rfind('.')]
            databaseFilenames.append(dataName + '.csv')

    print(databaseFilenames)
    return databaseFilenames

# step #2 - import csv module we created earlier
# put headers separately into one list and data in another list
# source: https://www.geeksforgeeks.org/python-read-csv-column-into-list-without-header/


def fromCSVtoList(databaseFilenames):
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
    print("The number of entries in data is ", len(data))
    return [columns, data]


def sparkDFandAVG(columns, data):
    # step #2.1 - follow the steps in this guide to install hadoop (and the correct version of Java) on your machine
    # source: https://medium.com/analytics-vidhya/hadoop-how-to-install-in-5-steps-in-windows-10-61b0e67342f8
    # step #2.2 - follow the steps in this guide to install pySpark on your machine
    # source: https://gongster.medium.com/how-to-use-pyspark-in-pycharm-ide-2fd8997b1cdd
    # step # 2.9 - use time measurement capability per https://docs.python.org/3/library/time.html
    # step #3 - start a spark session from pyspark.sql module
    # source: https://www.geeksforgeeks.org/find-minimum-maximum-and-average-value-of-pyspark-dataframe-column/

    # create spark session using the 'oceanspark' name
    #print("create spark session")
    #startTime = time.monotonic()
    spark = SparkSession.builder.appName('oceanspark').getOrCreate()
    #endTime = time.monotonic()
    #print("spark session made, in ", (endTime - startTime), " s")

    # Step #4 - create and act on the dataframe repeatedly,
    #   each time using a larger portion of the dataset up to the dataset's actual size

    # hard-coding for output sorting reasons, can automate if more data points needed
    fractions = ([1, 5], [1, 4], [1, 3], [2, 5], [1, 2], [3, 5], [2, 3], [3, 4], [4, 5], [1, 1])

    # create storage list n-tuples
    timeMeasures = []
    # assign variable a value for safety
    numPartitions = 0
    maxPartitions = 0
    entries = len(data)

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
        # record the largest/longest number of partitions
        if numPartitions > maxPartitions:
            maxPartitions = numPartitions

        # Step #5 - limiting number of partitions that the operation can run on
        # source: https://towardsdatascience.com/how-to-efficiently-re-partition-spark-dataframes-c036e8261418
        for activePartitions in range(1, numPartitions+1):
            # for creating "broken" record with fewer max partitions
            # if (number[1] == 5) and (activePartitions > 4):
            #     continue

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

        # for local machine testing with small datasets
        # if number[1] == 2: #maxRows > 10000:
        #     break

    # we now have all the time data in one place
    return [timeMeasures, maxPartitions]
    # for measure in timeMeasures:
        # print("for rows read ", measure[0], " and ", measure[1], " partitions:  time to create dataframe: ", measure[2], " time to avg slice of DF: ", measure[3])
        # print(measure)
    # save this raw data to an output file


def outputToFile(timeMeasures):
    rawdata = np.asarray(timeMeasures)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    np.savetxt("coalesce_raw"+timestamp+".csv", rawdata, delimiter=",", header="rows read,amount partitions,dataframe creation time,dataframe averaging time")
    return timestamp

def prepListForGraphing(timeMeasures, maxPartitions):
    # print("preparing data for graphing")
    # with all raw data saved, prepare data for graphing by filling gaps with None
    # matplotlib can skip over None data safely but can't handle different X and Y sizes

    # determine how many unique numbers of lines were read
    # Can't just use fractions because it might have terminated early

    # X axis displays number of lines read
    uniqueLines = []
    for measure in timeMeasures:
        if measure[0] not in uniqueLines:
            uniqueLines.append(measure[0])
    # check to see if uniqueLines populated correctly
    # print("uniqueLines ", len(uniqueLines), uniqueLines)

    # find how big the timeMeasurements would be if it had no gaps
    totalEntries = (len(uniqueLines) * maxPartitions) - 1
    lastItem = len(timeMeasures) - 1

    for item in range(totalEntries):
        # prevent going beyond the end of array
        if item == lastItem -1:
            timeMeasures.append([timeMeasures[item][0],timeMeasures[item][1]+1, None, None])
        # if the number of lines read changes
        # BUT the number of partitions isn't the same as the peak,
        # then there is a gap that needs to be padded
        elif (timeMeasures[item][0] != timeMeasures[item+1][0]) and timeMeasures[item][1] < maxPartitions:
            #insert a new entry to maintain dimensions
            timeMeasures.insert(item+1, [ timeMeasures[item][0], timeMeasures[item][1]+1, None, None ] )

    # it always overshoots by one
    timeMeasures.pop()
    # our data should have same dimensions and gaps filled by None
    # for measure in timeMeasures:
        # print("for rows read ", measure[0], " and ", measure[1], " partitions:  time to create dataframe: ", measure[2], " time to avg slice of DF: ", measure[3])
        # print(measure)
    return timeMeasures


def separateAxes(timeMeasures, maxPartitions):
    # go through timeMeasures by index
    # create number of y measurements equal to numPartitions

    # X axis displays number of lines read
    xAxis = []
    # Y axis displays time, but must have multiple arrays, DF creation and AVG operation
    yDF = []
    yAVG = []

    # number of partitions for data point = array index (1 part at index 1, 2 at 2, etc)
    for amtPartitions in range(maxPartitions+1):
        yDF.append([])
        yAVG.append([])

    # check to see if prepared for population correctly
    # print("yDF ", len(yDF), yDF)
    # print("yAVG ", len(yAVG), yAVG)

    curPos = 0
    endPos = len(timeMeasures)
    for curPos in range(endPos):
        #print(type(timeMeasures[curPos][0]), " ", timeMeasures[curPos][0])
        #print("rows read: ", timeMeasures[curPos][0], " partitions : ", timeMeasures[curPos][1], " creating wholeDF: ", timeMeasures[curPos][2], " time to avg reduced DF: ", timeMeasures[curPos][3])
        # keep track of what amt of partition was being used
        parts = (curPos+1) % maxPartitions
        if parts == 0:
            parts = maxPartitions
        # print("curPos ", curPos, " partitions ", parts)
        # only measure the xAxis every time it changes
        if parts == 1:
            #print(timeMeasures[curPos])
            xAxis.append(timeMeasures[curPos][0])
        yDF[parts].append(timeMeasures[curPos][2])
        #print(timeMeasures[curPos])
        yAVG[parts].append(timeMeasures[curPos][3])
        #print(timeMeasures[curPos])

    return [xAxis, yDF, yAVG]

# check that transfer from timeMeasures to multiple different arrays worked
# print("xAxis ", xAxis)
# print("yDF ", len(yDF))
# for entry in yDF:
#     print(entry)
# print("yAVG ", len(yAVG))
# for entry in yAVG:
#     print(entry)


def graphData(xAxis, yDF, yAVG, timestamp):
    # Step #6 - having gathered the data, visualize it for ease of understanding
    # source: https://matplotlib.org/stable/tutorials/introductory/pyplot.html
    # source: https://stackoverflow.com/questions/4971269/how-to-pick-a-new-color-for-each-plotted-line-within-a-figure-in-matplotlib

    colors = cm.rainbow(np.linspace(0, 1, len(yDF)))
    plt.figure(num=1, figsize=[10, 8])
    dataSlices = len(yDF) -1
    # print("xAxis ", len(xAxis), " yDF ", len(yDF), " yAVG ", len(yAVG), " yAVG[0] ", len(yAVG[1]), len(yAVG[2]), " colors ", len(colors))

    # plot 1 line per amt of partitions used
    for index in range(dataSlices):
        plt.plot(xAxis, yDF[index+1], c=colors[index], marker="o", label=(index+1, 'partitions'))

    plt.xlabel('Lines Read')
    plt.ylabel('Time (seconds)')
    plt.title("time to create dataframe, coalesce()")
    plt.legend(bbox_to_anchor=(1.0, 1.0))
    plt.savefig("coalesce_DFgraph_" + timestamp + ".png", bbox_inches='tight')
    # dataframe graph saved

    plt.figure(num=2, figsize=[10, 8])

    # plot 1 line per amt of partitions used
    for index in range(dataSlices):
        plt.plot(xAxis, yAVG[index+1], c=colors[index],  marker="o", label=(index+1, 'partitions'))
        #print(index, " ", colors[index])

    plt.xlabel('Lines Read')
    plt.ylabel('Time (seconds)')
    plt.title("time to average dataframe, coalesce()")
    plt.legend(bbox_to_anchor=(1.0, 1.0))
    plt.savefig("coalesce_AVGgraph_" + timestamp + ".png", bbox_inches='tight')
    # averaging time graph saved

def main():
    # extract filenames from list of database URLs
    files_to_read = getCSVfilenames()
    # read CSVs into a list, headers stored in [0] and body stored in [1]
    stored_data = fromCSVtoList(files_to_read)
    # perform Spark-related actions on the stored data,
    # list of measurements stored in [0] and highest num. of partitions used in [1]
    time_measurements = sparkDFandAVG(stored_data[0], stored_data[1])
    # output raw data to output file, timestamp is used elsewhere
    timestamp = outputToFile(time_measurements[0])
    # proofread, fill gaps in data with None for graphing purposes
    gap_filled_measurements = prepListForGraphing(time_measurements[0], time_measurements[1])
    # list of measurements should now have all gaps filled by None,
    # the graphing application can't handle odd lengths but can handle None entries
    axes_x_and_2_ys = separateAxes(gap_filled_measurements, time_measurements[1])
    # x-axis stored in [0], y for dataframe creation at [1], y for time to finish averaging [2]
    # pass separated axes and timestamp to be graphed
    graphData(axes_x_and_2_ys[0], axes_x_and_2_ys[1], axes_x_and_2_ys[2], timestamp)


# run program :)
main()