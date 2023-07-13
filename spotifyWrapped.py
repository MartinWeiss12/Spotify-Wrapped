import os
import pytz
import time
import pandas as pd
from datetime import datetime

# path of your folder with all endsong json files
path = ''
# path of where the excel files will be saved to
output_path = ''

files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.json')]
data = pd.concat([pd.read_json(file) for file in files])

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
#start_date = '2022-12-31'
#filtered_data = filtered_data[filtered_data['ts'] >= start_date]

# filter TO a date
#end_date = '2022-09-26'
#filtered_data = filtered_data[filtered_data['ts'] <= end_date]

streaming_log = filtered_data[['ts', 'master_metadata_track_name', 'master_metadata_album_album_name', 'master_metadata_album_artist_name', 'spotify_track_uri']]
streaming_log.columns = ['Timestamp', 'Track', 'Album', 'Artist', 'URI']
streaming_log = streaming_log.sort_values('Timestamp')

ts_list = streaming_log['Timestamp'].tolist()
track_list = streaming_log['Track'].tolist()
uri_list = streaming_log['URI'].tolist()

streaming_log = streaming_log.drop(columns=['URI'], axis=1)
streaming_log.to_excel(f'{output_path}spotify_streaming_log.xlsx', index=False)

print('Minutes Played: {:,}'.format(filtered_data['ms_played'].sum() // 60000))

count = 0
count_list = [0]
index_list = []
unique_uri_list = []

for i in range(len(uri_list) - 1):
	if (uri_list[i] == uri_list[i+1]):
		count = count + 1
		index_list.append(i)
		unique_uri_list.append(uri_list[i])
		count_list.append(count)
	else:
		count_list.append(1)
		count = 0
mpi = (count_list.index(max(count_list)))
print('On', ts_list[mpi - max(count_list)], 'you started playing', track_list[mpi] + '.', 'After playing it', max(count_list), 'times in a row, you stopped playing it at', ts_list[mpi])

track_artist_df = pd.DataFrame(list(zip(
	filtered_data['master_metadata_track_name'], 
	filtered_data['master_metadata_album_artist_name'],
	filtered_data['spotify_track_uri'])),
	columns=['Track', 'Artist', 'URI']
)

track_artist_df = (
	track_artist_df[~track_artist_df[['Track', 'Artist', 'URI']]
	.apply(lambda x: 'None' in x.values, axis=1)]
	.reset_index(drop=True)
)

track_artist_df['URI'] = track_artist_df['URI'].str.replace('spotify:track:', '')

artist_counts = track_artist_df['Artist'].value_counts()
top_artists_df = pd.DataFrame({'Artist': artist_counts.index, 'Streams': artist_counts.values})

# print top 100 tracks
rows_to_print = 100
# print ALL tracks
# rows_to_print = top_artists_df.shape[0]
pd.set_option('display.max_rows', None)
top_artists = top_artists_df.head(rows_to_print).reset_index().rename(columns={'index': 'Rank'})
top_artists['Rank'] += 1
print(top_artists.to_string(index=False))

top_artists.to_excel(f'{output_path}top_artists.xlsx', index=False)

track_artist_df['Duplicate URI'] = track_artist_df['URI']
groups = track_artist_df.groupby(['Track', 'Artist'])

for group_name, group_df in groups:
	if len(group_df) > 1:
		unique_uris = group_df['URI'].unique()
		track_artist_df.loc[group_df.index, 'Duplicate URI'] = unique_uris[0]

track_artist_df['URI'] = track_artist_df['Duplicate URI']
track_artist_df.drop('Duplicate URI', axis=1, inplace=True)

count_df = track_artist_df['URI'].value_counts().reset_index()
grouped_df = track_artist_df.groupby('URI').agg({'Track': lambda x: ', '.join(x.unique()), 'Artist': lambda x: ', '.join(x.unique())}).reset_index()
merged_df = count_df.rename(columns={'URI': 'Streams', 'index': 'URI'}).merge(grouped_df, on='URI')
merged_df = merged_df[['URI', 'Track', 'Artist', 'Streams']]
merged_df = merged_df.drop('URI', axis=1)

# print top 100 tracks
rows_to_print = 100
# print ALL tracks
# rows_to_print = merged_df.shape[0]
pd.set_option('display.max_rows', None)
top_tracks = merged_df.head(rows_to_print).reset_index().rename(columns={'index': 'Rank'})
top_tracks['Rank'] += 1
print(top_tracks.to_string(index=False))
top_tracks.to_excel(f'{output_path}top_tracks.xlsx', index=False)