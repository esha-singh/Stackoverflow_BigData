from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "testkeysppace"
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace(KEYSPACE)

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