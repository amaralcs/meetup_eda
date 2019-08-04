from MeetupDownload import MeetupDownload
import time

TEST_LIST = []


def testAnnouncer(test_name):
	print("\n" + "-"*20, test_name, "-"*20)


def testGetPeerMeetups(meetup_download, target):
	peer_mtps = meetup_download.getPeerMeetups(target)


def testGetMeetupMembers(meetup_download, target):
	member_df = meetup_download.getMeetupMembers(target, timer=1)
	print(f"Fetched {member_df['id'].nunique()} members from {target}.\n")
	print(f"Created DataFrame with shape {member_df.shape[0]}")


if __name__ == '__main__':
	print("Initiating tests on MeetupDownload...")
	api_key = "6968702120622427b7a4337f232316"
	target = 'pyladiesdublin'
	meetup_download = MeetupDownload(api_key)

	if 1 in TEST_LIST:
		testAnnouncer("GetPeerMeetups")
		testGetPeerMeetups(meetup_download, target)
		time.sleep(.5)
	if 2 in TEST_LIST:
		testAnnouncer("GetMeetupMembers")
		testGetMeetupMembers(meetup_download, target)
		time.sleep(.5)