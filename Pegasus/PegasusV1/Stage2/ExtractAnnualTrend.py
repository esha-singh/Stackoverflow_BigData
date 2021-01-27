from datetime import datetime
import os
import sys
import CassandraHelper
import xml.etree.ElementTree as xml
import time

start_time = time.time()

def extract_info_from_source(source_path):
    annual_tags_trend = {}

    context = xml.iterparse(source_path, events=("start", "end"))
    context = iter(context)
    event, root = context.next()

    for event, elem in context:
        if event == "end":
            if elem is not None and elem.attrib.has_key('Tags') and elem.attrib.has_key('CreationDate'):
                elem_tags = elem.attrib['Tags'][1:-1].split('><')
                post_creation_date = datetime.strptime(elem.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
                for tag in elem_tags:
                    if not annual_tags_trend.has_key(post_creation_date.year):
                        annual_tags_trend[post_creation_date.year] = {}
                    if not annual_tags_trend[post_creation_date.year].has_key(tag):
                        annual_tags_trend[post_creation_date.year][tag] = 1
                    else:
                        annual_tags_trend[post_creation_date.year][tag] += 1
                del elem_tags
                del post_creation_date
            root.clear()

    return annual_tags_trend

if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1 and args[1] == 'ssd':
        data_source = "D://BigData//Stage2_data//"
    else:
        data_source = "C://Users//PranayDev//Documents//BigData//ETL//Pegasus//Stage2//Stage2_data//"

    posts_file_name_suffix = "\\Posts.xml"
    walk = os.walk(data_source)
    data_source_directories = [x[0] for x in walk]
    data_source_directories = data_source_directories[1:]

    print("Starting processing for " + str(len(data_source_directories)) + " sources")
    for data_source in data_source_directories:
        data_source_path = data_source + posts_file_name_suffix
        annual_tags_trend = extract_info_from_source(data_source_path)
        domain_name = data_source.split("//")[-1].split(".")[0]
        for year in annual_tags_trend:
            CassandraHelper.insert_values_in_annual_trends_column_family(domain_name, year, annual_tags_trend[year])
        print("Loaded " + domain_name)

    print("--- %s seconds to finish ---" % (time.time() - start_time))