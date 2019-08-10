# Meetup vars
API_KEY = "6968702120622427b7a4337f232316"
TARGET = 'pyladiesdublin'

# Group data vars
DIM_COLS = ['organizer', 'category', 'meta_category', 'pro_network']
DROPPED_COLS = ['created', 'next_event','group_photo', 'key_photo', 'timezone']
USER_DROPPED_COLS = ['photo', 'self', 'other_services']

# Output location
FILESTORE = './filestore'

GROUP_FOLDER = FILESTORE + "/group"
USER_FOLDER = FILESTORE + "/user"

GROUP_DATA_FNAME = GROUP_FOLDER + "/fact_group_dataset.csv"
GROUP_DIM_FNAME = GROUP_FOLDER + "/dim_"
USER_DATA_FNAME = USER_FOLDER + "/fact_user_dataset.csv"
TOPIC_DATA_FNAME = USER_FOLDER + "/dim_topics.csv"

