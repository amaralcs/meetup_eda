import pandas as pd
import os, shutil
import warnings
warnings.simplefilter(action='ignore')

import transform.transform_group_data as tgd
from settings import TARGET, DROPPED_COLS, GROUP_DATA_FNAME, DIM_COLS, FILESTORE

DEV_MODE = False

def delete_existing_files():
		folder = FILESTORE
		for the_file in os.listdir(folder):
			file_path = os.path.join(folder, the_file)
			try:
				if os.path.isfile(file_path):
					os.unlink(file_path)
			except Exception as e:
				print(e)


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

	if DEV_MODE:
		delete_existing_files()

	print("Concatenating DFs...")
	for group in similar_dict:
		fact_df = pd.concat([fact_df, similar_dict[group]['fact_df']])
		dim_list = [
			pd.concat([tgt_dim, sim_dim])
			for tgt_dim, sim_dim in zip(dim_list, similar_dict[group]['dim_df_list'])
		]

	try:
	 	open(GROUP_DATA_FNAME)
	 	print(f"File {GROUP_DATA_FNAME} already exists, appending to it...")
	 	mode = 'a'
	except Exception as e:
	 	print(f"Creating files...")
	 	mode = None

	group_transformer.write_results(fact_df, GROUP_DATA_FNAME, mode)
	for dim_df, dim_name in zip(dim_list, DIM_COLS):
		dim_file = FILESTORE+f"/dim_{dim_name}.csv"
		group_transformer.write_results(dim_df, dim_file, mode)

if __name__ == '__main__':

	group_transformer = tgd.TransformGroupData(TARGET)

	transform_group(group_transformer)

	print("Complete.")


