import pandas as pd

import meetup_download.MeetupDownload as mtd
from settings import API_KEY, GROUP_DATA_FNAME

DIM_COLS = ['organizer', 'category', 'meta_category', 'pro_network']
DROPPED_COLS = ['created', 'next_event','group_photo', 'key_photo', 'timezone']


def createDimensionDF(df):
	"""
	Creates dimension tables based on columns listed in DIM_COLS

	:param df: Target group DF as returned by MeetupDownload.getTargetMeetup
	"""
	dims_df = df.loc[:, DIM_COLS]
	return [dims_df[dims_df[col].notnull()][col] for col in DIM_COLS]


## TO DO: Add method to clean up description

def createFactDF(df):
	"""
	Using the remaining columns, creates a DF for a group. 
	Should consist of a single row

	:param df: Target group DF as returned by MeetupDownload.getTargetMeetup
	"""
	fact_cols = [c for c in df.columns if c not in DIM_COLS]
	return pd.DataFrame(df.loc['id', fact_cols]).T.reset_index(drop=True)


if __name__ == '__main__':
	target = 'pyladiesdublin'
	meetup_download = mtd.MeetupDownload(API_KEY)

	print(f"Transforming DFs for {target}")
	target_df = (meetup_download
		.getTargetMeetup(target)
		.drop(DROPPED_COLS, axis=1)
	)

	dim_list = createDimensionDF(target_df)
	fact_df = createFactDF(target_df)

	print(f"Dim list has {len(dim_list)} dataframes with lengths:")
	print([len(dim) for dim in dim_list])

	print(f"Fact table has shape {fact_df.shape}.")
	print("Transformation completed...")

	fact_df.to_csv(GROUP_DATA_FNAME)
	## TO DO: figure out how I want to write the dims as well.