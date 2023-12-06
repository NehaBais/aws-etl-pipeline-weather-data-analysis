import csv
import os


files = [os.path.join("staging_data", f"{i}.csv") for i in range(2010, 2023)]

with open(os.path.join("staging_data", "data.csv"), "w") as datafile:
    writer = csv.writer(datafile)
    for i, filepath in enumerate(files):
        with open(filepath, "r") as csvfile:
            reader = csv.reader(csvfile)
            if i != 0:
                _ = next(reader, None)
            for line in reader:
                writer.writerow(line)
