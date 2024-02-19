import os
import pytz
import time
import requests
import pandas as pd
from datetime import datetime

# your client id
client_id = ''
# your client secret
client_secret = ''

token_url = 'https://accounts.spotify.com/api/token'
token_data = {'grant_type': 'client_credentials'}
token_response = requests.post(token_url, auth=(client_id, client_secret), data=token_data)
access_token = token_response.json()['access_token']
headers = {'Authorization': f'Bearer {access_token}'}

# path of your folder with all endsong json files
path = ''
# path of where the excel files will be saved to
output_path = ''

files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.json')]
data_frames = [pd.read_json(file).astype({col: 'float64' for col in pd.read_json(file).select_dtypes(include='bool').columns}) for file in files]
data = pd.concat(data_frames)

# filter out streams less than 30 seconds
filtered_data = data[data['ms_played'] >= 30000].reset_index(drop=True)
# drop rows that do not have a URI
filtered_data = filtered_data[filtered_data['spotify_track_uri'].notnull()]

timezone = pytz.timezone('US/Eastern')
datetime.now(tz=timezone)

filtered_data['ts'] = pd.to_datetime(filtered_data['ts'], utc=True)
filtered_data['ts'] = filtered_data['ts'].dt.tz_convert(timezone)
filtered_data['ts'] = filtered_data['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')

# filter FROM a date
#start_date = '2023-01-01'
#filtered_data = filtered_data[filtered_data['ts'] >= start_date]

# filter TO a date
# end_date = '2022-01-01'
# filtered_data = filtered_data[filtered_data['ts'] <= end_date]

streaming_log = filtered_data[['ts', 'master_metadata_track_name', 'master_metadata_album_album_name', 'master_metadata_album_artist_name', 'spotify_track_uri']]
streaming_log.columns = ['Timestamp', 'Track', 'Album', 'Artist', 'URI']
streaming_log = streaming_log.sort_values('Timestamp')

ts_list = streaming_log['Timestamp'].tolist()
track_list = streaming_log['Track'].tolist()
uri_list = streaming_log['URI'].tolist()

streaming_log = streaming_log.drop(columns=['URI'], axis=1)
streaming_log.to_excel(f'{output_path}spotify-streaming-log.xlsx', index=False)

print('Minutes Played: {:,}'.format(filtered_data['ms_played'].sum() // 60000))

mpi = 0
max_count = 0
unique_uri_list = []
for i in range(len(uri_list) - 1):
    if uri_list[i] == uri_list[i+1]:
        count = count + 1
        if count > max_count:
            max_count = count
            mpi = i
        unique_uri_list.append(uri_list[i])
    else:
        count = 0
        
start_ts = ts_list[mpi - max_count]
track = track_list[mpi]
times_played = max_count
end_ts = ts_list[mpi]
print(f'You played {track} {times_played} times in a row from {start_ts} to {end_ts}.')

cleaned_df = pd.DataFrame(list(zip(
	filtered_data['master_metadata_track_name'],
	filtered_data['master_metadata_album_album_name'],
	filtered_data['master_metadata_album_artist_name'],
	filtered_data['spotify_track_uri'])),
	columns=['Track', 'Album', 'Artist', 'Track URI']
)

cleaned_df = (
	cleaned_df[~cleaned_df[['Track', 'Album', 'Artist', 'Track URI']]
	.apply(lambda x: 'None' in x.values, axis=1)]
	.reset_index(drop=True)
)

cleaned_df['Track URI'] = cleaned_df['Track URI'].str.replace('spotify:track:', '')

track_uris = cleaned_df['Track URI']
unique_track_uris = list(set(track_uris))
n_unique_track_sublists = len(unique_track_uris) // 20 + (len(unique_track_uris) % 20 > 0)
unique_track_sublists = [unique_track_uris[i * 20 : (i + 1) * 20] for i in range(n_unique_track_sublists)]
track_artist_album_df = pd.DataFrame(columns=['Track URI', 'Artist URI', 'Album URI', 'Album Image URL'])
start_time = time.time()

for track_list in unique_track_sublists:
    params = {'ids': ','.join(track_list)}
    response = requests.get('https://api.spotify.com/v1/tracks', headers=headers, params=params)
    if response.status_code == 200:
            data = response.json()
            for track in data['tracks']:
                try:
                    track_uri = track['uri'].replace('spotify:track:', '')
                    artist_uri = track['artists'][0]['id']
                    album_uri = track['album']['uri'].replace('spotify:album:', '')
                    album_image_url = track['album']['images'][0]['url']
                    new_row = pd.Series({'Track URI': track_uri, 'Artist URI': artist_uri, 'Album URI': album_uri, 'Album Image URL': album_image_url})
                    track_artist_album_df = pd.concat([track_artist_album_df, new_row.to_frame().T], ignore_index=True)
                except:
                     continue
    
    elapsed_time = time.time() - start_time
    if elapsed_time >= 27:
        print('Waiting for 32 seconds...')
        time.sleep(32)
        start_time = time.time()
        print('Timer reset.')

artist_uris = track_artist_album_df['Artist URI']
unique_artist_uris = list(set(artist_uris))
n_unique_artist_sublists = len(unique_artist_uris) // 50 + (len(unique_artist_uris) % 50 > 0)
unique_artist_sublists = [unique_artist_uris[i * 50 : (i + 1) * 50] for i in range(n_unique_artist_sublists)]
unique_artist_image_url_dict = {}
start_time = time.time()

for artist_list in unique_artist_sublists:
    params = {'ids': ','.join(artist_list)}
    response = requests.get('https://api.spotify.com/v1/artists?', headers=headers, params=params)
    if response.status_code == 200:
            data = response.json()
            for artist in data['artists']:
                try:
                    artist_uri = artist['uri'].replace('spotify:artist:', '')
                    artist_image_url = artist['images'][0]['url']
                    unique_artist_image_url_dict[artist_uri] = artist_image_url
                except:
                     continue

    elapsed_time = time.time() - start_time
    if elapsed_time >= 27:
        print('Waiting for 32 seconds...')
        time.sleep(32)
        start_time = time.time()
        print('Timer reset.')

track_artist_album_df['Artist Image URL'] = track_artist_album_df['Artist URI'].map(unique_artist_image_url_dict)
spotify_data = pd.merge(cleaned_df, track_artist_album_df, on='Track URI', how='left')

def get_top_100(count_type, spotify_data):
    
    if count_type == 'Track':
        spotify_data[['Track', 'Album']] = spotify_data[['Track', 'Album']].apply(lambda x: x.str.replace('Feat', 'feat'))
        track_artist_groups = spotify_data.groupby(['Track', 'Artist'])
        songs_dict = {}
        for group, group_df in track_artist_groups:
            max_uri = group_df['Track URI'].value_counts().idxmax()
            group_df['Track URI'] = max_uri
            songs_dict[group] = group_df
        spotify_data = pd.concat(songs_dict.values(), ignore_index=True)
        
    uri_counts = spotify_data[f'{count_type} URI'].value_counts().reset_index(name='Streams')
    uri_counts.columns = [f'{count_type} URI', 'Streams']
    spotify_data = pd.merge(spotify_data, uri_counts, on=f'{count_type} URI', how='left')
    spotify_data = spotify_data.drop_duplicates(subset=f'{count_type} URI')
    spotify_data.sort_values(by='Streams', ascending=False, inplace=True, ignore_index=True)
    top_100 = spotify_data.head(100).copy().assign(Rank=lambda x: range(1, len(x) + 1))
    top_100['Streams'] = top_100['Streams'].astype(int)
    return top_100

top_100_tracks = get_top_100('Track', spotify_data)[['Rank', 'Track', 'Album', 'Artist', 'Streams']]
top_100_tracks.to_excel(f'{output_path}top-100-tracks.xlsx', index=False)
top_100_artists = get_top_100('Artist', spotify_data)[['Rank', 'Artist', 'Streams', 'Artist Image URL']]
top_100_artists.to_excel(f'{output_path}top-100-artists.xlsx', index=False)
top_100_albums = get_top_100('Album', spotify_data)[['Rank', 'Album', 'Artist', 'Streams', 'Album Image URL']]
top_100_albums.to_excel(f'{output_path}top-100-albums.xlsx', index=False)