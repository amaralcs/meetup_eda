import pandas as pd
from itertools import chain
from collections import defaultdict
import warnings
warnings.simplefilter(action='ignore')

import download.meetup_download as mtd

from settings import API_KEY, TARGET, FILESTORE, USER_DATA_FNAME

class TransformUserData():

	def make_topic_df(self, topic_list):
		"""
		Receives a list of dictionaries, creates a temporary dict from them
		and then conver to DataFrame.

		:param topic_list: List of dictionaries containing information on topics of interest to the user
		"""
		temp_dict = defaultdict(list)
		for entry in chain(topic_list):
			for k,v in entry.items():
				temp_dict[k].append(v)
		df = pd.DataFrame(temp_dict)
		df.columns = [col if col!='id' else 'topic_id' for col in df.columns]
		return df


	def join_topics(self, df):
		"""
		Converts a Series of topic_ids of interest to user into a string.

		:param df: topic_df as created when applying make_topic_df to topic Series
		"""
		try:
			ls = [str(i) for i in df['topic_id']]
		except Exception as e:
			return ''
		return ','.join(ls)

	def __init__(self, target, api_key=API_KEY, filestore=FILESTORE, \
		user_data_fname=USER_DATA_FNAME):

		self.target = target
		self.api_key = api_key
		self.filestore = filestore
		self.user_data_fname = user_data_fname

		self.meetup_download = mtd.MeetupDownload(api_key)
