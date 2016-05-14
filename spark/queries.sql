/* Main query to aggregate the whole thing (no partition): */
select player_table.player, l.x, 
l.y, l.cnt from (select player_id, x, y, count(*) cnt from 
	(select  player_id, round(x, 0) x, round(y, 0) y from location_table
where 
player_id <> -1) a 
group by player_id, x, y) l 
left join players 
on l.player_id = players.player_id order by name, x, y limit 20;

/* Main query to aggregate the whole thing (partition by player_id): */
select concat(players.first_name,'_', players.last_name) name, l.x, 
l.y, l.cnt from (select player_id, x, y, count(*) cnt from (select 
player_id, round(x, 0) x, round(y, 0) y from locations_part where 
player_id <> -1) a group by player_id, x, y) l left join players 
on l.player_id = players.player_id order by name, x, y limit 20;