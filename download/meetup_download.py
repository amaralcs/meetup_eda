import pandas as pd
import time

import meetup.api


class MeetupDownload():
	"""
	Class:	MeetupDownload
	Author: Carlos Amaral
	Date:	04/08/2019
	"""

	def group_to_df(self, group):
		return pd.DataFrame.from_dict(group.__dict__)


	def create_member_df(self, member_list):
		"""
		Support function to iterate through a list of members and create a dataframe

		:param member_list: A list of members of a meetup as created by get_meetup_members
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


	def results_to_df(self, df_list):
		df = pd.DataFrame()
		for d in df_list:
			df = pd.concat([df, pd.DataFrame(d)])
		return df


	def get_all_results(self, target, function, timer=3):
		"""
		Method to get all results from an API call.
		Since the API only returns 200 results at a time, it is necessary to repeat the call
		while adjusting the offset until no results are returned. 

		:param target: Name of the Meetup of interest
		:param function: API function to be called
		:param timer: Time (in seconds) to wait before issue a new API call
		"""
		def get_meetup_members(target, offset):
			"""
			Method to get members that a part of target Meetup.

			:param target: The Meetup of interest
			:param offset: Current value of offset for API call
			"""
			return (
		    	self.client
		    	.GetMembers(group_urlname=target, offset=offset)
		    	.__dict__['results']
		    )


		def get_meetup_events(target, offset):
			"""
			Method to get past and upcoming events of a meetup. See Python API doc for 
			other event statuses

			:param target: The Meetup of interest
			:param offset: Current value of offset for API call
			"""
			return (
				self.client
				.GetEvents(group_urlname=target, offset=offset, \
					status="past,upcoming")
				.__dict__['results']
			)

		api_list = [] 
		offset = 0

		api_call = {
			'getMembers' : get_meetup_members,
			'getEvents' : get_meetup_events
		}


		while True:
		    result = api_call[function](target,offset)

		    if len(result) == 0:
		        break
		    else:
		        api_list.append(result)
		        offset = offset+1

		    # Set a timer to avoiding bombarding API with calls 
		    time.sleep(timer)

		result_list = [
			result 
			for call_result in api_list 
			for result in call_result
		]

		if function == 'getMembers':
			return self.create_member_df(result_list)	
		else:
			return self.results_to_df(result_list)
		

	def get_target_meetup(self, target):
		"""
		Creates a dataframe with information on a target group

		:param target: Name of the meetup with data of interest
		"""
		tgt_group = self.client.GetGroup(urlname=target.lower())
		df = self.group_to_df(tgt_group)

		return df


	def get_peer_meetups(self, target):
		"""
		Creates a list of dataframes with information on similar meetups to target

		:param target: The meetup of interest
		"""
		similar_groups = self.client.GetGroupSimilarGroups(urlname=target.lower())
		df_list = []
		for gp in similar_groups:
			df_list.append(self.group_to_df(gp))

		if len(df_list)==0:
			print("No similar meetups were found")
			df_list.append(pd.DataFrame())
		else:
			pass
			# print(f"Found {len(df_list)} groups similar to {target}")
		
		return df_list


	def __init__(self, api_key, client=None):
		self.api_key = api_key
		self.client = meetup.api.Client(api_key)
	