import os
import re
import csv
import shutil


correct_dir = 'correct'
broken_dir = 'broken'

# create `correct` and `broken` directories
if not os.path.exists(correct_dir):
    os.mkdir(correct_dir)

if not os.path.exists(broken_dir):
    os.mkdir(broken_dir)


# set path and other global variables
path = 'pp10Hz_202/'
broken_files = []
correct_files = []


lantitude_pattern = '^(\+|-)?(?:90(?:(?:\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,6})?))$'
longitude_pattern = '^(\+|-)?(?:180(?:(?:\.0{1,6})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,6})?))$'

compiled_lantitude_pattern = re.compile(lantitude_pattern)
compiled_longitude_pattern = re.compile(longitude_pattern)


def valid_latitude(latitude):
    return compiled_lantitude_pattern.search(latitude)


def valid_longitude(longitude):
    return compiled_longitude_pattern.search(longitude)


# loop over csv files
for filename in os.listdir(path):
    file_path = f'{path}{filename}'
    with open(file_path) as csvfile:
        print(f'[*] processing file: {filename}')

        reader = csv.DictReader(csvfile, delimiter=',')

        broken = False
        records = 0
        for row in reader:
            latitude = row['latitude']
            longitude = row['longitude']

            # check if pattern for latitude and longitude is valid
            if not (valid_latitude(latitude) and valid_latitude(latitude)):
                broken = True
                break

            records += 1

        if records == 0 or broken:
            # file is either empty or contains none valid records
            # move file to `broken` directory
            shutil.move(file_path, broken_dir)
            broken_files.append(filename)
            print(f'[-] {filename} is broken and has been moved to `{broken_dir}` directory\n')
        else:
            # file contain valid records, so move it to `correct` directory
            shutil.move(file_path, correct_dir)
            correct_files.append(filename)
            print(f'[+] {filename} contains valid records and has been moved to `{correct_dir}` directory\n')

        records = 0

print('\n\n[-] Broken files are:')
for filename in broken_files:
    print(f'  {filename}')


print('\n\n[+] Correct files are:')
for filename in correct_files:
    print(f'  {filename}')

print(f'\n\nGo checkout `{correct_dir}` and `{broken_dir}` directories')
