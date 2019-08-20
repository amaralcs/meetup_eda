# Meetup vars
API_KEY = "6968702120622427b7a4337f232316"
TARGET = 'pyladiesdublin'

# Group data vars
DIM_COLS = ['organizer', 'category', 'meta_category', 'pro_network']
DROPPED_COLS = ['created', 'next_event','group_photo', 'key_photo', 'timezone']
USER_DROPPED_COLS = ['photo', 'self', 'other_services']
EVENT_DROPPED_COLS = ['created', 'event_url', 'group','how_to_find_us', 'photo_url']
EVENT_DROPPED_ROWS = ['urlname', 'who', 'repinned', 'group_lat', 'group_lon', 'created', 'join_mode', 'count', 'average']

# Output location
FILESTORE = './filestore'

GROUP_FOLDER = FILESTORE + "/group"
USER_FOLDER = FILESTORE + "/user"
EVENT_FOLDER = FILESTORE + "/events"

GROUP_DATA_FNAME = GROUP_FOLDER + "/fact_group_dataset.csv"
GROUP_DIM_FNAME = GROUP_FOLDER + "/dim_"
USER_DATA_FNAME = USER_FOLDER + "/fact_user_dataset.csv"
TOPIC_DATA_FNAME = USER_FOLDER + "/dim_topics.csv"
EVENT_DATA_FNAME = EVENT_FOLDER + "/fact_events.csv"
VENUE_DATA_FNAME = EVENT_FOLDER + "/dim_venues.csv"
