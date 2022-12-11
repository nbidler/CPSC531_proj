# CPSC531_proj
holding area for CPSC 531 big data semester project

changelog:
11/25 - added .py file that imported nc data to csv, then imported the data into a data list and header list, then created spark session to calculate avg

12/11 - post-submission update, ouch
  on python 3.7 and installing dependency libraries, run "python openDAP-to-CSV.py" to download raw data from remote storage to local storage
  run main_repartition.py or main_coalesce.py to benchmark pySpark performance against different data sizes and with different amounts of partitions
    (note: there is a line near the start of the program you can alter to change how much of the total dataset is used)
    # you can have as many or few files as you want to read for raw data,
    # but know that 12 is the minimum required for the data prep needed for graph outputs
    return databaseFilenames[0:12]
    # or comment the above line out, and un-comment the below line
    # return databaseFilenames
