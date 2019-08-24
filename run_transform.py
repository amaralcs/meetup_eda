import pandas as pd
import os, shutil
import warnings
warnings.simplefilter(action='ignore')

import transform.transform_group_data as tgd
import transform.transform_user_data as tud
import transform.transform_event_data as ted
from utils import write_results
from settings import TARGET, DROPPED_COLS, GROUP_DATA_FNAME, GROUP_DIM_FNAME, \
	DIM_COLS, USER_DROPPED_COLS, USER_DATA_FNAME, TOPIC_DATA_FNAME, \
	EVENT_DROPPED_COLS, EVENT_DROPPED_ROWS, EVENT_DATA_FNAME, VENUE_DATA_FNAME

DEV_MODE = False


def transform_group(group_transformer, target=TARGET):
	"""
	Method for downloading, transforming, and saving locally data from a target
	meetup group.

	:param group_transformer: Instance of TransformGroupData class initialised with API key.
	:param target: Meetup group of interest.
	"""

	def get_similar_groups(n=10):
		"""
		Creates a dictionary with similar groups to target meetup. 
		Each inner dict contains a fact_df for the group as well as a list of
		dim_dfs.

		The meetup API only returns 5 similar groups at a time, hence we make the 
		call n times to fetch more groups.

		:param n: Number of times to fetch similar groups 
		"""
		ten_similar = [
			(sim_gp, sim_gp.loc['id', 'urlname'])
			for i in range(n)
			for sim_gp in group_transformer.meetup_download.get_peer_meetups(target)
		]
		similar_dict = {
			group_name : {
				'fact_df' : tgd.TransformGroupData(target).create_fact_df(df),
				'dim_df_list' : tgd.TransformGroupData(target).create_dimension_df(df)
			}
			for (df, group_name) in ten_similar
		}
		return similar_dict


	print(f"Transforming DFs for {target}")
	target_df = (
		group_transformer
		.meetup_download
		.get_target_meetup(target)
		.drop(DROPPED_COLS, axis=1)
	)
	dim_list = group_transformer.create_dimension_df(target_df)
	fact_df = group_transformer.create_fact_df(target_df)

	print(f"Dim list has {len(dim_list)} dataframes with lengths:")
	print([len(dim) for dim in dim_list])
	print(f"Fact table has shape {fact_df.shape}.")
	print("Transformation completed...")

	print("Fetching similar groups")
	similar_dict = get_similar_groups()

	print("Concatenating DFs...")
	for group in similar_dict:
		fact_df = pd.concat([fact_df, similar_dict[group]['fact_df']])
		dim_list = [
			pd.concat([tgt_dim, sim_dim])
			for tgt_dim, sim_dim in zip(dim_list, similar_dict[group]['dim_df_list'])
		]

	write_results(fact_df, GROUP_DATA_FNAME, dev_mode=DEV_MODE)
	for dim_df, dim_name in zip(dim_list, DIM_COLS):
		dim_file = GROUP_DIM_FNAME +f"{dim_name}.csv"
		write_results(dim_df, dim_file, dev_mode=DEV_MODE)


def transform_user_data(user_transformer,target=TARGET):

	print("Fetching data...")
	user_df = (
		user_transformer
		.meetup_download
		.get_all_results(target, 'getMembers')
		.drop(USER_DROPPED_COLS, axis=1)
	)

	print(f"Transforming dataframes for users of {target}:")
	print("Fetching all topics")
	all_topics = pd.concat([
		df
		for df in user_df['topics'].apply(user_transformer.make_topic_df)
	])

	print("Creating table with distinct topics of interest")
	unique_ids = all_topics['topic_id'].unique()
	unique_mask = all_topics['topic_id'].isin(unique_ids)
	unique_topics = all_topics[unique_mask].reset_index(drop=True)
	unique_topics['target'] = target

	print("Transforming topics in user_df...")
	user_topics = user_df.copy()
	user_topics['topics'] = user_topics['topics'].apply(user_transformer.make_topic_df)
	user_topics['topics'] = user_topics['topics'].apply(user_transformer.join_topics)
	user_topics.columns = [
		col 
		if col!='id' else 'user_id' 
		for col in user_topics.columns
	]

	write_results(user_topics, USER_DATA_FNAME, dev_mode=DEV_MODE)
	write_results(unique_topics, TOPIC_DATA_FNAME, dev_mode=DEV_MODE)


def transform_events(event_transformer, target=TARGET):

	print("Fetching Event data")
	events_df = (
		event_transformer
		.meetup_download
		.get_all_results(TARGET, "getEvents")
		.drop(EVENT_DROPPED_COLS, axis=1)
	)

	print("Updating column names")
	cols = [c if c!= 'id' else 'event_id' for c in events_df.columns]
	cols = [c if c!= 'venue' else 'venue_id' for c in cols]
	events_df.columns = cols

	print("Creating events table...")
	fact_events = event_transformer.make_event_df(events_df)

	print("Creating venues table...")
	dim_venues = event_transformer.make_venue_df(events_df)

	write_results(fact_events, EVENT_DATA_FNAME, dev_mode=DEV_MODE)
	write_results(dim_venues, VENUE_DATA_FNAME, dev_mode=DEV_MODE)

if __name__ == '__main__':

	group_transformer = tgd.TransformGroupData(TARGET)
	transform_group(group_transformer)

	user_transformer = tud.TransformUserData(TARGET)
	transform_user_data(user_transformer)

	event_transformer = ted.TransformEventData(TARGET)
	transform_events(event_transformer)
	print("Transformations complete.")

