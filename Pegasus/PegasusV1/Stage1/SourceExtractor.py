import os
import sys
import xml.etree.ElementTree as xml
from urlparse import urlparse
from pyunpack import Archive
import random

if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1 and args[1] == 'ssd':
        source_meta_path = "D://BigData//Stage1_data//Sites.xml"
        zip_files_path_prefix = "D://BigData//Stage1_data//"
        data_destination_directory = "D://BigData//Stage2_data//"
    else:
        source_meta_path = "C://Users//PranayDev//Documents//BigData//ETL//Pegasus//Stage1//Stage1_data//Sites.xml"
        zip_files_path_prefix = "C://Users//PranayDev//Documents//BigData//ETL//Pegasus//Stage1//Stage1_data//"
        data_destination_directory = "C://Users//PranayDev//Documents//BigData//ETL//Pegasus//Stage2//Stage2_data//"
    
    file_extension = ".7z"
    source_folder_path = ""
    domains = []
    zip_file_paths = []

    context = xml.iterparse(source_meta_path, events=("start", "end"))
    context = iter(context)
    event, root = context.next()
    for event, elem in context:
        if event == "end":
            if elem.attrib.has_key('Url') and "meta." not in elem.attrib['Url']:
                domains.append(elem.attrib['TinyName'])
                zip_file_paths.append(urlparse(elem.attrib['Url']).hostname)

    domains.remove("stackoverflow")
    zip_file_paths.remove("stackoverflow.com")
    zip_file_paths = zip_file_paths[23:]
    if not len(args) > 1 or not args[1] == 'ssd':
        # too big for my machine, only trying random 10 sources
        zip_file_paths = random.sample(zip_file_paths, 10)

    for zip_file in zip_file_paths:
        print("Started extracting file: "+zip_file)
        path_to_zip_file = zip_files_path_prefix + zip_file + file_extension
        directory_to_extract_to = data_destination_directory + zip_file
        os.mkdir(directory_to_extract_to)
        Archive(path_to_zip_file).extractall(directory_to_extract_to)
        print("Processed extracting file: "+zip_file)