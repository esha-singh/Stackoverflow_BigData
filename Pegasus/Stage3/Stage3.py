from datetime import datetime
import os
import sys
import CassandraHelper
import xml.etree.ElementTree as xml
import time

class Stage3:
    def extract_posts_aggregate_info_from_source(self, source_path):
        unanswered_questions = 0
        total_questions = 0
        answered_questions = 0
        questions_with_answers_count = 0
        answers_count = 0
        all_accepted_answers_ids = {} #{answer_id -> question_timestamp}
        all_accepted_answers_duration = []
        most_viewed_posts = {}
        most_scored_posts = {}
        all_tags = {}

        context = xml.iterparse(source_path, events=("start", "end"))
        context = iter(context)
        event, root = context.next()

        # iterate over each xml element
        for event, elem in context:
            if event == "end":
                if elem is not None:
                    if elem.attrib.has_key('PostTypeId') and elem.attrib['PostTypeId'] == '1':
                        total_questions += 1
                        # if a question doesn't have AcceptedAnswerId, increment unanswered count 
                        if not elem.attrib.has_key('AcceptedAnswerId'):
                            unanswered_questions += 1
                        else:
                            # if it is answered populate answerid -> questionCreationDate map
                            all_accepted_answers_ids[elem.attrib['AcceptedAnswerId']] = elem.attrib['CreationDate']
                        if elem.attrib.has_key('AnswerCount'):
                            questions_with_answers_count += 1
                            answers_count += int(elem.attrib['AnswerCount'])
                    # create maps for viewCount, score and tags
                    if elem.attrib.has_key('Id') and elem.attrib.has_key('ViewCount'):
                        most_viewed_posts[elem.attrib['Id']] = int(elem.attrib['ViewCount'])
                    if elem.attrib.has_key('Id') and elem.attrib.has_key('Score'):
                        most_scored_posts[elem.attrib['Id']] = int(elem.attrib['Score'])
                    if elem.attrib.has_key('Tags'):
                        elem_tags = elem.attrib['Tags'][1:-1].split('><')
                        for tag in elem_tags:
                            if all_tags.has_key(tag):
                                all_tags[tag] += 1
                            else:
                                all_tags[tag] = 1
                root.clear()

        context = xml.iterparse(source_path, events=("start", "end"))
        context = iter(context)
        event, root = context.next()

        # second iteration to calculate average time to answer a question
        for event, elem in context:
            if event == "end":
                # if the element is an answer, calculate difference answer creation date - question creation creation date
                if elem is not None and elem.attrib.has_key('Id') and elem.attrib['Id'] in all_accepted_answers_ids.keys():
                    question_date_time = datetime.strptime(all_accepted_answers_ids[elem.attrib['Id']], '%Y-%m-%dT%H:%M:%S.%f')
                    answer_date_time = datetime.strptime(elem.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
                    time_to_answer = answer_date_time - question_date_time
                    all_accepted_answers_duration.append(time_to_answer.days)
                    del all_accepted_answers_ids[elem.attrib['Id']]
                root.clear()

        # sort the maps to get ids with maximum votes
        sorted_tags = sorted(all_tags.items() ,  key=lambda x: x[1] )
        sorted_tags.reverse()
        trending_tags = [i[0] for i in sorted_tags][0:10]
        trending_tags = ",".join(trending_tags)
        most_scored_posts = sorted(most_scored_posts.items() ,  key=lambda x: x[1] )
        most_scored_posts.reverse()
        most_scored_posts = [i[0] for i in most_scored_posts][0:10]
        most_scored_posts = ",".join(most_scored_posts)
        most_viewed_posts = sorted(most_viewed_posts.items() ,  key=lambda x: x[1] )
        most_viewed_posts.reverse()
        most_viewed_posts = [i[0] for i in most_viewed_posts][0:10]
        most_viewed_posts = ",".join(most_viewed_posts)
        average_answers_count = int(answers_count/questions_with_answers_count)
        # caclulate average time to answer
        average_time_to_answer = int(sum(all_accepted_answers_duration)/len(all_accepted_answers_duration))

        del sorted_tags
        del all_accepted_answers_duration
        del all_accepted_answers_ids

        return total_questions, unanswered_questions, trending_tags, average_answers_count, most_viewed_posts, most_scored_posts, average_time_to_answer

    def extract_annual_tag_trend_from_source(self, source_path):
        annual_tags_trend = {}

        context = xml.iterparse(source_path, events=("start", "end"))
        context = iter(context)
        event, root = context.next()

        # for each element in xml, populate annual_tags_trend[{year}][{tag}]
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

    def load_aggregate_posts_data_from_source(self, data_source):
        start_time = time.time()

        # get all domain directories from source
        posts_file_name_suffix = "\\Posts.xml"
        walk = os.walk(data_source)
        data_source_directories = [x[0] for x in walk]
        data_source_directories = data_source_directories[1:]

        print("Starting processing for " + str(len(data_source_directories)) + " sources")
        # for each domain, extract posts info and dump into cassandra
        for data_source in data_source_directories:
            data_source_path = data_source + posts_file_name_suffix
            domain_name = data_source.split("//")[-1].split(".")[0]
            print("Loading " + domain_name)
            total_questions, unanswered_questions, trending_tags, average_answers_count, most_viewed_posts, most_scored_posts, average_time_to_answer = self.extract_posts_aggregate_info_from_source(data_source_path)
            CassandraHelper.insert_values_in_posts_column_family(domain_name, total_questions, unanswered_questions, trending_tags, average_answers_count, most_viewed_posts, most_scored_posts, average_time_to_answer)
            print("Loaded " + domain_name)

        print("--- %s seconds to finish ---" % (time.time() - start_time))

    def load_annual_topics_trends_from_source(self, data_source):
        start_time = time.time()

        # get all domain directories from source
        posts_file_name_suffix = "\\Posts.xml"
        walk = os.walk(data_source)
        data_source_directories = [x[0] for x in walk]
        data_source_directories = data_source_directories[1:]

        print("Starting processing for " + str(len(data_source_directories)) + " sources")
        # for each domain, extract top trends and dump into cassandra
        for data_source in data_source_directories:
            data_source_path = data_source + posts_file_name_suffix
            annual_tags_trend = self.extract_annual_tag_trend_from_source(data_source_path)
            domain_name = data_source.split("//")[-1].split(".")[0]
            for year in annual_tags_trend:
                CassandraHelper.insert_values_in_annual_trends_column_family(domain_name, year, annual_tags_trend[year])
            print("Loaded " + domain_name)

        print("--- %s seconds to finish ---" % (time.time() - start_time))

if __name__ == "__main__":
    args = sys.argv
    # if ssd argument is passed, use it as a source and destination for data
    if len(args) > 1 and args[1] == 'ssd':
        data_source = "D://BigData//Stage3_data//"
    else:
        data_source = "C://Users//PranayDev//Documents//BigData//ETL//Pegasus//Stage3//Stage3_data//"
    
    stage3 = Stage3()

    stage3.load_aggregate_posts_data_from_source(data_source)
    stage3.load_annual_topics_trends_from_source(data_source)
