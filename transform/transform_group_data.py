import pandas as pd

import download.meetup_download as mtd
from settings import API_KEY, FILESTORE, GROUP_DATA_FNAME, DIM_COLS

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

		group_id = df.loc['id', 'id']
		# Make a temporary single row DF to store the group ID
		temp = pd.DataFrame(
			[[str(group_id)]*len(DIM_COLS)],
			columns=DIM_COLS,
			index=['group_id']
		)
		dims_df = pd.concat([temp, df.loc[:, self.dim_cols]])
		dims_df.index = [i if i != 'id' else 'dim_id' for i in dims_df.index]

		dim_list = [dims_df[dims_df[c].notnull()][c] for c in self.dim_cols]

		return [make_target_col(dim_df) for dim_df in dim_list]


	## TO DO: Add method to clean up description

	def create_fact_df(self, df):
		"""
		Using the remaining columns, creates a DF for a group. 
		Should consist of a single row

		:param df: Target group DF as returned by MeetupDownload.get_target_meetup
		"""
		fact_cols = [c for c in df.columns if c not in self.dim_cols]
		fact_df = pd.DataFrame(df.loc['id', fact_cols]).T.reset_index(drop=True)
		fact_df['target_meetup'] = self.target
		fact_df.index = [i if i != 'id' else 'group_id' for i in fact_df.index]
		return fact_df


	def write_results(self, df, fname, mode):
		if mode=='a':
			with open(fname, mode) as output:
				df.to_csv(output, header=False)
		else:
			df.to_csv(fname)


	def __init__(self, target, api_key=API_KEY, filestore=FILESTORE, \
				 group_data_fname=GROUP_DATA_FNAME, dim_cols=DIM_COLS):
		self.api_key = api_key,
		self.filestore = filestore,
		self.group_data_fname = group_data_fname
		self.target = target
		self.dim_cols = dim_cols

		self.meetup_download = mtd.MeetupDownload(api_key)

