from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "testkeysppace"
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace(KEYSPACE)

def create_posts_column_family():
    session.execute("""
        CREATE COLUMNFAMILY StackExchangePosts
            (
                Domain TEXT PRIMARY KEY
                ,TotalQuestions INT
                ,UnansweredQuestions INT
                ,TrendingTags TEXT
                ,AverageAnswersCount INT
                ,MostViewedPosts TEXT
                ,MostScoredPosts TEXT
                ,AverageTimeToAnswer INT
            );
    """)

def create_annual_trends_family():
    session.execute("""
        CREATE COLUMNFAMILY AnnualTrends
            (
                Domain TEXT
                ,Year INT
                ,tags map<TEXT, INT>
                , PRIMARY KEY (Domain, Year)
            );
    """)

def insert_values_in_posts_column_family(domain, total_questions, unanswered_questions, trending_tags, average_answers_count,
most_viewed_posts, most_scored_posts, average_time_to_answer):
    session.execute("""
        INSERT INTO StackExchangePosts 
        (Domain,TotalQuestions, UnansweredQuestions, TrendingTags, AverageAnswersCount, MostViewedPosts, MostScoredPosts, AverageTimeToAnswer)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (domain, total_questions, unanswered_questions, trending_tags, average_answers_count, most_viewed_posts, most_scored_posts, average_time_to_answer))

def insert_values_in_annual_trends_column_family(domain, year, tags):
    session.execute("""
        INSERT INTO AnnualTrends 
        (Domain, Year, Tags)
        VALUES
        (%s, %s, %s);
    """, (domain, year, tags))

def get_annual_trend_tag_by_year(domain, year):
    result = session.execute("""
        SELECT Tags
        FROM AnnualTrends
        WHERE Domain = %s AND Year = %s;
    """, (domain, year))
    
    return result

def get_all_domains():
    result = session.execute("""
        SELECT domain
        FROM AnnualTrends
    """)
    
    return result

def cleanup_domain_keys():
    result = session.execute("""
        SELECT *
        FROM AnnualTrends
    """)
    updates = 0
    for row in result:
        if row.domain.find("Stage2_data") > -1:
            session.execute("""
                DELETE FROM AnnualTrends
                Where domain='"""+ row.domain +"""'
            """)
            updates += 1
    print("Total updates "+str(updates))

def create_posts_column_family2():
    session.execute("""
        CREATE COLUMNFAMILY PostInfo
            (
                Domain TEXT PRIMARY KEY
                ,TotalQuestions INT
                ,UnansweredQuestions INT
                ,TrendingTags LIST<TEXT>
                ,AverageAnswersCount INT
                ,MostViewedPosts LIST<INT>
                ,MostScoredPosts LIST<INT>
                ,AverageTimeToAnswer INT
            );
    """)

def insert_values_in_posts_column_family2(domain, total_questions, unanswered_questions, trending_tags, average_answers_count,
most_viewed_posts, most_scored_posts, average_time_to_answer):
    session.execute("""
        INSERT INTO PostInfo 
        (Domain,TotalQuestions, UnansweredQuestions, TrendingTags, AverageAnswersCount, MostViewedPosts, MostScoredPosts, AverageTimeToAnswer)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (domain, total_questions, unanswered_questions, trending_tags, average_answers_count, most_viewed_posts, most_scored_posts, average_time_to_answer))


def loadPostInfoFromCsv(csvFile):
    import csv
    with open(csvFile) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        csv.DictReader
        for row in csv_reader:
            insert_values_in_posts_column_family2(row[0], int(row[5]), int(row[7]), row[6].split(','), int(row[1]), list(map(int, row[4].split(','))), list(map(int, row[3].split(','))), int(row[2]))

def loadAnnualTrendsFromCSV(csvFile):
    import csv
    import ast
    with open(csvFile) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        csv.DictReader
        for row in csv_reader:
            tags = ast.literal_eval(row[2])
            insert_values_in_annual_trends_column_family2(row[0], int(row[1]), tags)

def insert_values_in_annual_trends_column_family2(domain, year, tags):
    session.execute("""
        INSERT INTO AnnualTrends2
        (Domain, Year, Tags)
        VALUES
        (%s, %s, %s);
    """, (domain, year, tags))

# create_posts_column_family2()
# loadPostInfoFromCsv("C:\\export.csv")
# loadAnnualTrendsFromCSV('C:\\annualTrends.csv')