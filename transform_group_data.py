import pandas as pd

import meetup_download.MeetupDownload as mtd
from settings import API_KEY, FILESTORE, GROUP_DATA_FNAME

DIM_COLS = ['organizer', 'category', 'meta_category', 'pro_network']
DROPPED_COLS = ['created', 'next_event','group_photo', 'key_photo', 'timezone']


class TransformGroupData():
	
	def create_dimension_df(df):
		"""
		Creates dimension tables based on columns listed in DIM_COLS

		:param df: Target group DF as returned by MeetupDownload.getTargetMeetup
		"""
		dims_df = df.loc[:, DIM_COLS]
		return [dims_df[dims_df[col].notnull()][col].T for col in DIM_COLS]


	## TO DO: Add method to clean up description

	def create_fact_df(df):
		"""
		Using the remaining columns, creates a DF for a group. 
		Should consist of a single row

		:param df: Target group DF as returned by MeetupDownload.getTargetMeetup
		"""
		fact_cols = [c for c in df.columns if c not in DIM_COLS]
		return pd.DataFrame(df.loc['id', fact_cols]).T.reset_index(drop=True)


	def write_results(df, fname, mode='a'):
		if mode=='a':
			with open(fname, mode) as output:
				df.to_csv(output, header=False)
		else:
			df.to_csv(fname)


	if __name__ == '__main__':
		target = 'pyladiesdublin'
		meetup_download = mtd.MeetupDownload(API_KEY)

		print(f"Transforming DFs for {target}")
		target_df = (meetup_download
			.getTargetMeetup(target)
			.drop(DROPPED_COLS, axis=1)
		)

		dim_list = create_dimension_df(target_df)
		fact_df = create_fact_df(target_df)

		print(f"Dim list has {len(dim_list)} dataframes with lengths:")
		print([len(dim) for dim in dim_list])

		print(f"Fact table has shape {fact_df.shape}.")
		print("Transformation completed...")

		write_results(fact_df, GROUP_DATA_FNAME, mode=None)
		for dim_df, dim_name in zip(dim_list, DIM_COLS):
			write_results(dim_df, FILESTORE+f"/dim_{dim_name}")