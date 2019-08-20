import re
import pandas as pd
import warnings
warnings.simplefilter(action='ignore')

import download.meetup_download as mtd

from settings import API_KEY, TARGET, FILESTORE, EVENT_DATA_FNAME, VENUE_DATA_FNAME

TAG_RE = re.compile(r'<[^>]+>')

class TransformEventData():

	def make_event_df(self, df):
		"""
		Creates a fact table with infromation on past and upcoming events.

		:param df: Target event DF as returned by MeetupDownload.get_all_results('getEvents')
		"""
		def remove_tags(text):
			"""
			Helper function that removes HTML tags from a string

			:param text: Piece of text to remove HTML from.
			"""
			return TAG_RE.sub('', text)

		
		event_df = df.loc['id', :].reset_index(drop=True)

		event_not_detailed = (
			event_df['description']
			.str
			.contains('<p>Details coming soon...</p>')
		)
		filtered_events = event_df[~event_not_detailed]
		filtered_events['description'] = (
			filtered_events['description']
			.apply(lambda x: remove_tags(x))
		)
		return filtered_events


	def make_venue_df(self, df):
		"""
		Creates a separate table with information on the venues the meetup have been hosted

		:param df: A filtered version of target event DF as returned by MeetupDownload.get_all_results('getEvents')
		"""
		venues = pd.DataFrame(df.loc[:, 'venue_id'].reset_index())
		venues['key'] = (venues.index/10).astype(int)

		gb_venue = (venues
			.groupby(['key', 'index'])['venue_id']
			.aggregate('first')
			.unstack()
			.reset_index(drop=True)
		)
		clean_venue = gb_venue[gb_venue.isnull().all(axis=1)==False]
		clean_venue.columns = [c if c!='id' else 'venue_id'  for c in clean_venue.columns]
		return clean_venue


	def __init__(self, target, api_key=API_KEY, filestore=FILESTORE, \
		event_data_fname=EVENT_DATA_FNAME, venue_data_fname=VENUE_DATA_FNAME):

		self.target = target
		self.api_key = api_key
		self.filestore = filestore
		self.event_data_fname = event_data_fname

		self.meetup_download = mtd.MeetupDownload(api_key)