from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.mllib.clustering import KMeans, KMeansModel, GaussianMixture
import numpy as np

sqlc = SQLContext()
sc = SparkContext()

players_raw = sc.textFile("s3://dcproject/2014-10-29/*_players.csv").repartition(100)


def parse_players(line):
    fields = line.split(",")
    player_id = int(fields[0])
    team = int(fields[5])
    player = fields[3] + " " + fields[1]
    position = str(fields[4])
    jersey = int(fields[2])
    return player_id,team,player,position,jersey

print players_raw.take(2)
players_info = players_raw.map(parse_players)

fields = [("player_id", IntegerType()), ("team_id", IntegerType()), ("player_name", StringType()), ("position",StringType()), ("jersey", IntegerType())]
player_schema = StructType([StructField(x[0], x[1], True) for x in fields])
schema_PlayersInfo = sqlc.createDataFrame(players_info, player_schema)
sqlc.registerDataFrameAsTable(schema_PlayersInfo, "player_table")


locations_raw = sc.textFile("s3://dcproject/2014-10-29/*_moments.csv.gz").repartition(100)
def parse_locations(line):
    fields = line.split(",")
    game_id = int(fields[0])
    quarter = int(fields[1])
    #unix_time = int(fields[2])
    #game_clock = float(fields[3])
    #shot_clock = float(fields[4])
    team_id = int(fields[5])
    player_id = int(fields[6])

    x = float(fields[7])
    y = float(fields[8])
    #z = float(fields[9])

    if quarter > 2:
        x = 94 - x
        y = 50 - y

    return game_id, team_id, player_id, x, y

location_info = locations_raw.map(parse_locations)

fields = [("game_id", IntegerType()), ("team_id", IntegerType()), ("player_id", IntegerType()), ("x", DoubleType()), ("y", DoubleType())]
location_schema = StructType([StructField(x[0], x[1], True) for x in fields])
schema_LocationInfo = sqlc.createDataFrame(location_info, location_schema)
sqlc.registerDataFrameAsTable(schema_LocationInfo, "location_table")


y = sc.parallelize([float(x) for x in range(51)])
x = sc.parallelize([float(x) for x in range(95)])

fields = [StructField("x", DoubleType(), True), StructField("y", DoubleType(), True)]
null_location_schema = sqlc.createDataFrame(x.cartesian(y),  StructType(fields))
sqlc.registerDataFrameAsTable(null_location_schema, "location_null")


null_query = """
select p.player_id, p.team_id, p.position, l.x, l.y from player_table p CROSS JOIN location_null l
ORDER BY player_id, x, y
"""
player_null = sqlc.sql(null_query)
sqlc.registerDataFrameAsTable(player_null, "player_null")

aggregate_locations = """
select  player_table.player_id, player_table.player_id, l.x, l.y, l.cnt from
(select player_id, x, y, count(*) cnt from (select  player_id, round(x, 0) x, round(y, 0) y from
location_table where player_id <> -1) a group by player_id, x, y) l left join
player_table on l.player_id = player_table.player_id order by player_id, x, y
"""

player_loc_count = sqlc.sql(aggregate_locations)
sqlc.registerDataFrameAsTable(player_loc_count, "player_loc_count")

aggregate_locations_full = """
select b.player_id, b.team_id, b.position, b.x, b.y, COALESCE(a.cnt,0) cnt from player_null b
LEFT JOIN player_loc_count a on a.x = b.x AND a.y = b.y AND a.player_id = b.player_id ORDER BY b.player_id, b.x, b.y
"""

player_loc_count = sqlc.sql(aggregate_locations_full)
sqlc.registerDataFrameAsTable(player_loc_count, "player_loc_count")
sqlc.cacheTable("player_loc_count")


def order_array(x):
    array = np.zeros([51, 95], dtype=int)
    for value in x[1]:
        array[value[1], value[2]] = value[0]
    return (x[0], array.flatten())

player_arrays = player_loc_count.map(lambda x: (x[0],[(x[5], x[4], x[3])])).reduceByKey(lambda a, b: a + b).map(order_array)
clusters = KMeans.train(player_arrays.map(lambda x: x[1]), 6, maxIterations=10, initializationMode="random")

def print_prediction(x):
    return x[0], clusters.predict(x[1])
print players_info.map(lambda x: (x[0], x[1:]))\
    .join(player_arrays.map(print_prediction)).map(lambda x: (x[1][0][2], x[1][1])).sortByKey().collect()