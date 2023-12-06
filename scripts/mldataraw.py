import csv
import sys
import os


datatype = sys.argv[1].upper()

files = [os.path.join("staging_data", f"{i}.csv") for i in range(2010, 2023)]

with open(os.path.join("staging_data", f"raw{datatype}.csv"), "w") as datafile:
    writer = csv.writer(datafile)
    for i, filepath in enumerate(files):
        with open(filepath, "r") as csvfile:
            reader = csv.reader(csvfile)
            if i != 0:
                _ = next(reader, None)
            else:
                writer.writerow([x for j, x in enumerate(next(reader, None)) if j != 2])
            for line in reader:
                if datatype in line:
                    writer.writerow([x for j, x in enumerate(line) if j != 2])
