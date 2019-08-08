import pandas as pd

import download.meetup_download as mtd

class TransformGroupData():

	def create_dimension_df(self, df):
		"""
		Creates dimension tables based on columns listed in DIM_COLS

		:param df: Target group DF as returned by MeetupDownload.getTargetMeetup
		"""
		def make_target_col(dim_df):
			"""
			Helper function to create target_meetup column, used to join each
			dim table to the fact table
			"""
			dim_df['target_meetup'] = self.target
			return pd.DataFrame(dim_df).T

		dims_df = df.loc[:, DIM_COLS]
		dim_list = [dims_df[dims_df[col].notnull()][col] for col in DIM_COLS]
		return [make_target_col(dim_df) for dim_df in dim_list]


	## TO DO: Add method to clean up description

	def create_fact_df(self, df):
		"""
		Using the remaining columns, creates a DF for a group. 
		Should consist of a single row

		:param df: Target group DF as returned by MeetupDownload.get_target_meetup
		"""
		fact_cols = [c for c in df.columns if c not in DIM_COLS]
		fact_df = pd.DataFrame(df.loc['id', fact_cols]).T.reset_index(drop=True)
		fact_df['target_meetup'] = self.target
		return fact_df


	def write_results(self, df, fname, mode):
		if mode=='a':
			with open(fname, mode) as output:
				df.to_csv(output, header=False)
		else:
			df.to_csv(fname)


	def __init__(self, api_key, filestore, group_data_fname, target, dim_cols):
		self.api_key = api_key,
		self.filestore = filestore,
		self.group_data_fname = group_data_fname
		self.target = target
		self.dim_cols = dim_cols

		self.meetup_download = mtd.MeetupDownload(api_key)
