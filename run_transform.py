import pandas as pd

import transform.transform_group_data as tgd
from settings import API_KEY, FILESTORE, GROUP_DATA_FNAME, TARGET, \
	DROPPED_COLS, DIM_COLS


def transform_group(group_transformer):
	print(f"Transforming DFs for {TARGET}")
	target_df = (
		group_transformer
		.meetup_download
		.get_target_meetup(TARGET)
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

	group_transformer = tgd.TransformGroupData(
		API_KEY, 
		FILESTORE,
		GROUP_DATA_FNAME,
		TARGET,
		DIM_COLS
	)

	transform_group(group_transformer)

	


