import pandas as pd

import transform.transform_group_data as tgd
from settings import TARGET


def transform_group(group_transformer, target=TARGET):
	"""
	Method for downloading, transforming, and saving locally data from a target
	meetup group
	"""
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

	group_transformer = tgd.TransformGroupData()

	group_transformer.meetup_download.get_peer_meetups
	# transform_group(group_transformer)

	


