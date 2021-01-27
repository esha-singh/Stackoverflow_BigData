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

def create_posts_column_family():
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