import pytz
import time
import os, os.path
import pandas as pd
from datetime import datetime
from IPython.display import display

start_time = datetime.now()
start_time = start_time.strftime('%H:%M:%S')
print('Start Time:', start_time)
startTimerSec = time.time()
path = r'' #path of your file with all endsong json files
outputPath = r'' #path where to output excel files
data = []
files = os.listdir(path)
fileCount = 0
for file in files:
	if file.endswith('json'):
		df = pd.read_json(path + '/endsong_' + str(fileCount) + '.json')
		fileCount = fileCount + 1
		data.append(df)
data = pd.concat(data)
data = data[data.ms_played >= 30000] #streams longer than 30 seconds
data.reset_index(drop = True, inplace = True)
for stream in data.index:
	if(data['spotify_track_uri'][stream] is None):
		data.drop([stream], axis = 0, inplace = True)
		
est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S'
for ind in data.index:
	date = (data['ts'][ind])
	holdTS = datetime(int(date[0:4]), int(date[5:7]),
		int(date[8:10]), int(date[11:13]),
		int(date[14:16]), int(date[17:19]), tzinfo = utc)
	estTS = holdTS.astimezone(est).strftime(fmt)
	data.at[ind, 'ts'] = estTS
dateList = data['ts'].tolist()
date = '2020-01-01 00:00:00'
#data = data[data.ts >= date] #streams from after Jan 01, 2020 12:00:00 AM
#data = data[data.ts <= date] #streams from before Jan 01, 2020 12:00:00 AM
msPlayedList = (data['ms_played']).tolist()
print('Minutes Played:', round(sum(msPlayedList)/60000))
data.rename(columns = {'ts':'Timestamp', 'master_metadata_track_name':'Track',
	'master_metadata_album_artist_name':'Artist', 'master_metadata_album_album_name': 'Album', 'spotify_track_uri':'URI'}, inplace = True)
data = data[['Timestamp', 'Track', 'Artist', 'Album', 'URI']]
data = (data.sort_values(by = 'Timestamp')).reset_index(drop = True)
for ind in data.index:
	trackUri = (data['URI'][ind])
	holdTrackUri = trackUri.replace('spotify:track:', '')
	data.at[ind, 'URI'] = holdTrackUri
uriList = data['URI'].tolist()
data.to_excel(f'{outputPath}/spotify_streaming_log.xlsx', index = False)

tsList = data['Timestamp'].tolist()
trackList = data['Track'].tolist()
count = 0
countList = [0]
indexList = []
uniqueUriList = []
for i in range(len(uriList) - 1):
	if (uriList[i] == uriList[i+1]):
		count = count + 1
		indexList.append(i)
		uniqueUriList.append(uriList[i])
		countList.append(count)
	else:
		countList.append(1)
		count = 0
mpi = (countList.index(max(countList)))
print('On', tsList[mpi - max(countList)], 'you started playing', trackList[mpi] + '.',
	'After playing it', max(countList), 'times in a row, you stopped playing it at', tsList[mpi])

topArtistsDf = pd.DataFrame(columns = ['Rank', 'Artist', 'Streams'])
topArtists = []
artistList = data['Artist'].tolist()
for i in range(100): #change to see different number of top artists
	def most_frequent(artistList):
		return max(set(artistList), key = artistList.count)
	topArtist = most_frequent(artistList)
	topArtistsDf = topArtistsDf.append({'Rank': i+1, 'Artist': topArtist, 'Streams': artistList.count(topArtist)}, ignore_index = True)
	topArtists.append(topArtist)
	artistList = [i for i in artistList if i != topArtist]
display(topArtistsDf.to_string(index = False))
topArtistsDf.to_excel(f'{outputPath}/spotify_top_artists.xlsx', index = False)

topAlbumsDf = pd.DataFrame(columns = ['Rank', 'Album', 'Streams'])
topAlbums = []
albumList = data['Album'].tolist()
for i in range(50): #change to see different number of top albums
	def most_frequent(albumList):
		return max(set(albumList), key = albumList.count)
	topAlbum = most_frequent(albumList)
	topAlbumsDf = topAlbumsDf.append({'Rank': i+1, 'Album': topAlbum, 'Streams': albumList.count(topAlbum)}, ignore_index = True)
	topAlbums.append(topAlbum)
	albumList = [i for i in albumList if i != topAlbum]
display(topAlbumsDf.to_string(index = False))
topAlbumsDf.to_excel(f'{outputPath}/spotify_top_albums.xlsx', index = False)

print('URI List Length:', len(uriList))
print('Starting URI Mapping: %s seconds' % round(time.time() - startTimerSec, 2))
artistList = data['Artist'].tolist()
for i in range(len(uriList)):
	for j in range(len(uriList)):
		if((trackList[i] == trackList[j]) 
			and (artistList[i] == artistList[j])
			and (uriList[i] != uriList[j])):
			uriList[i] = uriList[j]
matchedTrackUriDf = pd.DataFrame(list(zip(trackList, artistList, uriList)), columns = ['Track', 'Artist', 'URI'])
uniqueTrackUriDf = matchedTrackUriDf.groupby(['URI'], as_index = False).first()
done_uri_mapping = datetime.now()
done_uri_mapping = done_uri_mapping.strftime('%H:%M:%S')
print('Done URI Mapping:', done_uri_mapping)
print('Done URI Mapping: %s seconds' % round(time.time() - startTimerSec, 2))

uniqueTrackList = uniqueTrackUriDf['Track'].tolist()
uniqueArtistList = uniqueTrackUriDf['Artist'].tolist()
uniqueUriList = uniqueTrackUriDf['URI'].tolist()
uriList = matchedTrackUriDf['URI'].tolist()
rank = []
topTracks = []
topArtistsFromTracks = []
streamsFromTracks = []
topUris = []
elapsedTimeList = []
streamCountTimer = time.time()
for i in range (len(uniqueUriList)): #use this to get streams for every unique song streamed
	def most_frequent(uriList):
		return max(set(uriList), key = uriList.count)
	topUriIntermediate = most_frequent(uriList)
	for j in range(len(uniqueUriList)):
		if (topUriIntermediate == uniqueUriList[j]):
			topTrackIntermediate = (uniqueTrackList	[j])
			break
	for k in range(len(uniqueArtistList)):
		if (topUriIntermediate == uniqueUriList[k]):
			topTrackArtistIntermediate = (uniqueArtistList[k])
			break
	rank.append(i+1)
	topTracks.append(topTrackIntermediate)
	topArtistsFromTracks.append(topTrackArtistIntermediate)
	streamsFromTracks.append(uriList.count(topUriIntermediate))
	topUris.append(topUriIntermediate)
	uriList = [i for i in uriList if i != topUriIntermediate]
	elapsedTime = (time.time() - streamCountTimer)
	elapsedTimeList.append(elapsedTime)
	print(('Track Number:'), (i+1), ('--- %s seconds ---' % round(elapsedTime, 3)), 'Difference:',  round(elapsedTimeList[i] - elapsedTimeList[i-1], 3), 'seconds')
topTracksDf = pd.DataFrame(list(zip(rank, topTracks, topArtistsFromTracks, streamsFromTracks, topUris)), columns = ['Rank', 'Track', 'Artist', 'Streams', 'URI'])
display(topTracksDf.to_string(index = False))
topTracksDf.to_excel(f'{outputPath}/spotify_top_tracks.xlsx', index = False)

end_time = datetime.now()
end_time = end_time.strftime('%H:%M:%S')
print('Start Time:', start_time)
print('End Time:', end_time)