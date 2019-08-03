import meetup.api
import pandas as pd

class MeetupDownload():

	def groupToDF(self, group):
		return pd.DataFrame.from_dict(group.__dict__)

	def getTargetMeetup(self, target):
		"""
		Creates a dataframe with information on a target group

		:param target: Name of the meetup with data of interest
		"""
		tgt_group = self.client.GetGroup(urlname=target.lower())
		df = self.dictToDF(tgt_group)

		return df


	def getPeerMeetups(self, target):
		"""
		Creates a list of dataframes with information on similar meetups to target

		:param target: The meetup of interest
		"""
		similar_groups = self.client.GetGroupSimilarGroups(urlname=target.lower())
		df_list = []
		for gp in similar_groups:
			df_list.append(self.dictToDF(gp))

		if len(df_list)==0:
			print("No similar meetups were found")
			df_list.append(pd.DataFrame())
		else:
			print(f"Found {len(df_list)} groups similar to {target}")
		
		return df_list


	def getMeetupMembers(self, target):
		"""
		Fetches data on members of target meetup

		:param target: The meetup of interest
		"""
		member_list = client.GetMembers(group_urlname=target).__dict__['results']
		df = pd.DataFrame()


		""" TO DO: Fix this bit of code """
		for member in member_list:
			if df.empty:
				df = self.dictToDF(member)
			else:
				df = pd.concat(df, pd.DataFrame.from_dict(member), ignore_index=True)

		return df

	def getGroupEvents():
		pass


	def __init__(self, api_key, client=None):
		self.api_key = api_key
		self.client = meetup.api.Client(api_key)


if __name__ == '__main__':
	api_key = "6968702120622427b7a4337f232316"
	mtp_down = MeetupDownload(api_key)

	tgt = 'pyladiesdublin'
	peer_mtps = mtp_down.getPeerMeetups(tgt)

	