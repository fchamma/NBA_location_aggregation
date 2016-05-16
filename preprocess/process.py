from secret import *
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import tarfile
import re
import gzip
from cStringIO import StringIO
import json
import csv

conn = S3Connection(AWS_ACCESS, AWS_SECRET, host="s3-us-west-2.amazonaws.com")
bucket = conn.get_bucket("dcproject")
keys = bucket.list()

directory_pattern = re.compile(r'.{4}-.{2}-.{2}.tar\.gz')
play_pattern = re.compile(r'.+.json\.gz')

for key in bucket.list():
    if directory_pattern.match(key.name):
        fp = StringIO(key.get_contents_as_string())
        tar = tarfile.open(mode="r:tar", fileobj=StringIO(key.get_contents_as_string()))
        first = True
        current_game_id = False
        completed_players, completed_moments, player_rows, moment_rows = [], [], [], []

        game_pattern = re.compile(re.escape(key.name.split('.')[0]) + r'/(\d{10})$')

        game_ids = [x for x in tar.getnames() if game_pattern.match(x)]

        for game_directory in game_ids:
            completed_players, completed_moments, player_rows, moment_rows, first = [], [], [], [], True
            for member in tar.getnames():
                if re.match(re.escape(game_directory) + r'/game_\d{10}-play_\d{3}\.json\.gz', member):
                    if len(completed_moments) > 7500:
                        completed_moments = completed_moments[len(completed_moments)-7500:]

                    play = StringIO(tar.extractfile(member).read())
                    play_data = json.loads(gzip.GzipFile(fileobj=play).read())
                    game_id = game_directory.split('/')[1]

                    if first:
                        home_players = play_data['home']['players']
                        visitor_players = play_data['visitor']['players']

                        for player in home_players:
                            if player['playerid'] not in completed_players:
                                completed_players.append(player['playerid'])
                                player_rows.append(player.values() + [play_data['home']['teamid']])

                        for player in visitor_players:
                            if player['playerid'] not in completed_players:
                                completed_players.append(player['playerid'])
                                player_rows.append(player.values() + [play_data['home']['teamid']])

                        first = False

                    for moment in play_data['moments']:
                        if moment[1] not in completed_moments:
                            completed_moments.append(moment[1])
                            for players in moment[5]:
                                moment_rows.append([game_id] + moment[0:4] + players)

            moments = StringIO()
            moments_writer = csv.writer(moments)

            players = StringIO()
            players_writer = csv.writer(players)

            players_writer.writerows(player_rows)
            moments_writer.writerows(moment_rows)

            moments_compressed = StringIO()
            moments_string = gzip.GzipFile(fileobj=moments_compressed, mode='w').write(moments.getvalue())

            moment_k = Key(bucket)
            players_k = Key(bucket)

            moment_k.key = game_directory + '_moments.csv.gz'
            players_k.key = game_directory + '_players.csv'

            moment_k.set_contents_from_string(moments_compressed.getvalue())
            players_k.set_contents_from_string(players.getvalue())

            moment_k.set_acl('public-read')
            players_k.set_acl('public-read')
            print "written " + game_directory + '_moments.csv.gz'
