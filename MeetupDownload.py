import meetup.api
import pandas as pd
import time

class MeetupDownload():

	def groupToDF(self, group):
		return pd.DataFrame.from_dict(group.__dict__)


	def getTargetMeetup(self, target):
		"""
		Creates a dataframe with information on a target group

		:param target: Name of the meetup with data of interest
		"""
		tgt_group = self.client.GetGroup(urlname=target.lower())
		df = self.groupToDF(tgt_group)

		return df


	def getPeerMeetups(self, target):
		"""
		Creates a list of dataframes with information on similar meetups to target

		:param target: The meetup of interest
		"""
		similar_groups = self.client.GetGroupSimilarGroups(urlname=target.lower())
		df_list = []
		for gp in similar_groups:
			df_list.append(self.groupToDF(gp))

		if len(df_list)==0:
			print("No similar meetups were found")
			df_list.append(pd.DataFrame())
		else:
			print(f"Found {len(df_list)} groups similar to {target}")
		
		return df_list


	def createMemberDF(self, member_list):
		"""
		Support function to iterate through a list of members and create a dataframe

		:param member_list: A list of members of a meetup as created by getMeetupMembers
		"""
		df = pd.DataFrame()

		for member_id, member in enumerate(member_list):
		    member_df = (
		    	pd
		        .DataFrame(list(member.items()))
		        .set_index(pd.Index([member_id]*len(member)))
		        .pivot(columns=0, values=1)
		     )
		    df = pd.concat([df, member_df])
		    
		return df


	def getMeetupMembers(self, target, timer=5):
		"""
		Fetches data on members of target meetup. 
		Since the API only returns 200 results at a time, we have to repeatedly make calls
		until all members are fetched.

		:param target: The meetup of interest
		"""
		api_list = [] 
		offset = 0

		while True:
		    result = (
		    	self.client
		    	.GetMembers(group_urlname=target, offset=offset)
		    	.__dict__['results']
		    )

		    if len(result) == 0:
		        break
		    else:
		        api_list.append(result)
		        offset = offset+1

		    # Set a timer to avoiding bombarding API with calls 
		    time.sleep(timer)

		member_list = [
			member 
			for call_result in api_list 
			for member in call_result
		]

		return self.createMemberDF(member_list)

	def getGroupEvents():
		pass


	def __init__(self, api_key, client=None):
		self.api_key = api_key
		self.client = meetup.api.Client(api_key)
	