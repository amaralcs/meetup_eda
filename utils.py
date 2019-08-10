"""
File with utility functions
"""
import re

from settings import FILESTORE

def delete_existing_files(folder):
		for the_file in os.listdir(folder):
			file_path = os.path.join(folder, the_file)
			try:
				if os.path.isfile(file_path):
					os.unlink(file_path)
			except Exception as e:
				print(e)


def write_results(df, fname, mode=None, dev_mode=False):
	"""
	Utility function to create csv files from dfs

	:param df: The df to be outputted
	:param fname: Path and name of output csv
	:param mode: How to write the output. By default it tries to write a new file, use mode='a' to append to existing file.
	"""

	if dev_mode:
		print("-"*10, "DEV MODE IS ACTIVATED", "-"*10)
		print("Overwriting existing files")
		folder = re.search("((?:\\w+\\/){2})", fname).group()
		delete_existing_files()

	if mode == 'a':
		print(f"Appending to existing file: {fname}")
		with open(fname, mode) as output:
			df.to_csv(output, header=False)
	elif mode == None:
		print(f"Creating new file {fname}")
		df.to_csv(fname)
	else:
		print(f"This type of write operation is not permitted: {mode}")