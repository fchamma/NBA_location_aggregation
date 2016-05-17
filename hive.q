
/* Setting AWS keys to access data in S3 buckets */
-- SET fs.s3n.awsAccessKeyId = <secret key id>;
-- SET fs.s3n.awsSecretAccessKey = <secret access key>;

/* Setting parameters to properly partition data */
-- SET hive.exec.dynamic.partition=true;  
-- SET hive.exec.dynamic.partition.mode=nonstrict; 

CREATE EXTERNAL TABLE players (player_id STRING, team STRING, 
first_name STRING, last_name STRING, position STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://dcproject/players/';

CREATE EXTERNAL TABLE locations (row_number STRING, game_id STRING, 
unix_time STRING, quarter INT, team_id STRING, player_id STRING, 
game_clock FLOAT, shot_clock FLOAT, x FLOAT, y FLOAT, z FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3n://dcproject/locations/";

-- load data inpath 's3n://dcproject/players' overwrite into table players
-- load data inpath "s3://dcproject/locations" overwrite into table locations

-- chammas code

/* Setting AWS keys to access data in S3 buckets */
SET fs.s3n.awsAccessKeyId = ;
SET fs.s3n.awsSecretAccessKey = <secret access key>;

/* Setting parameters to properly partition data */
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 

CREATE EXTERNAL TABLE players (player_id STRING, team STRING, 
first_name STRING, last_name STRING, position STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://dcproject-public/players/';

CREATE EXTERNAL TABLE locations (row_number STRING, game_id STRING, 
unix_time STRING, quarter INT, team_id STRING, player_id STRING, 
game_clock FLOAT, shot_clock FLOAT, x FLOAT, y FLOAT, z FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://dcproject-public/locations/';

CREATE TABLE locations_part (game_id STRING, quarter INT, team_id 
STRING, game_clock FLOAT, shot_clock FLOAT, x FLOAT, y FLOAT, z FLOAT)
PARTITIONED BY (player_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOCATION 's3n://dcproject-public/locations_part2/';

INSERT OVERWRITE TABLE locations_part PARTITION (player_id)
SELECT game_id, quarter, team_id, game_clock, shot_clock, x, y, z, 
player_id FROM locations;

/* Main query to aggregate the whole thing (no partition): */
select concat(players.first_name,'_', players.last_name) name, l.x, 
l.y, l.cnt from (select player_id, x, y, count(*) cnt from (select 
player_id, round(x, 0) x, round(y, 0) y from locations where 
player_id <> -1) a group by player_id, x, y) l left join players 
on l.player_id = players.player_id order by name, x, y limit 20;

/* Main query to aggregate the whole thing (partition by player_id): */
select concat(players.first_name,'_', players.last_name) name, l.x, 
l.y, l.cnt from (select player_id, x, y, count(*) cnt from (select 
player_id, round(x, 0) x, round(y, 0) y from locations_part where 
player_id <> -1) a group by player_id, x, y) l left join players 
on l.player_id = players.player_id order by name, x, y limit 20;

