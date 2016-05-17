CREATE TABLE players (player_id INT, last_name varchar, jersey int, first_name VARCHAR, position VARCHAR, team_id int);

CREATE  TABLE locations (game_id int, quarter int, unix_time VARCHAR,
  game_clock FLOAT, shot_clock FLOAT, team_id INT, player_id INT, x FLOAT, y FLOAT, z FLOAT);

create table empty_court (x float, y float);

COPY players FROM '/Users/felipeformentiferreira/Documents/MSAN/spring_2016/distributed_comp/data/players/all_players.csv' DELIMITER ',' CSV;

COPY locations FROM '/Users/felipeformentiferreira/Documents/MSAN/spring_2016/distributed_comp/data/moments/all_moments.csv' DELIMITER ',' CSV;

COPY empty_court FROM '/Users/felipeformentiferreira/Documents/MSAN/spring_2016/distributed_comp/NBA_location_aggregation/court.csv' DELIMITER ',' CSV;


 -- Query time 70 seconds
select LHS.*, COALESCE(RHS.cnt, 0) as cnt from (select * from empty_court
cross JOIN (select DISTINCT(player_id) from players) as sub_query) as LHS
left JOIN (select players.player_id, concat(players.first_name,'_', players.last_name) as name, l.x,
l.y, l.cnt from (select player_id, x, y, count(*) cnt from (select
player_id, round(x) x, round(y) y from locations where
player_id <> -1) a group by player_id, x, y) l left join players
on l.player_id = players.player_id order by name, x, y) as RHS
    ON LHS.player_id = RHS.player_id and
       LHS.x = RHS.x and
       LHS.y = RHS.y;


