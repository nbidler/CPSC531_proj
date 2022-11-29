import os
import glob
import pandas as pd
import xarray as xr

# create the NC folder to hold our csvs so it doesn't clutter
try:
    # source: https://thispointer.com/how-to-create-a-directory-in-python/
    # make the directory, if it doesn't exist
    os.mkdir("NC")
except:
    # the directory is already made, there's no point making it
    pass

# create individual database files in each folder
with open('urls sorted.txt', 'r') as databaseURLlist:
    for url in databaseURLlist:
        print(url)
        dataName = url[url.rfind('/') +1:url.rfind('.')]
        print("grabbing and storing ", dataName)
        nc = xr.open_dataset(url)
        nc.to_dataframe().to_csv("NC/" + dataName + '.csv')
        print("finished with ", dataName)

# consolidate the CSVs we made using pandas!!
# source: https://www.freecodecamp.org/news/how-to-combine-multiple-csv-files-with-8-lines-of-code-265183e0854/

# change directory to NC, then use glob to get all .csv files
os.chdir("NC")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

# combine all csvs
print("combining all csv files")
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])

# export to the one csv to rule all
combined_csv.to_csv("combined ocean csv.csv")
print("done!")