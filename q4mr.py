from mrjob.job import MRJob
class q4MR(MRJob):
	def mapper(self, key, line):
		if not (line.startswith("ITIN_ID,YEAR,QUARTER,ORIGIN,ORIGIN_STATE_NM,DEST,DEST_STATE_NM,PASSENGERS")):
			l = line.split(",")
			origin = l[3]
			dest = l[5]
			itin = l[0]
			if origin != dest:
				yield itin, (origin, dest)
	def reducer(self, key, values):
		for origin, dest in values:
			yield key, (origin, dest)
if __name__ == '__main__':
	q4MR.run()
