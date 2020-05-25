from mrjob.job import MRJob
from mrjob.step import MRStep

class lab3_2(MRJob):
	
	def steps(self):
		return [
			MRStep(mapper=self.map_get_ratings,
				reducer=self.reduce_count_ratings),
			MRStep(
			reducer=self.sort	
			)
		]

	def map_get_ratings(self, _, line):
		(user_id, movie_id, rating, timestamp)=line.split("\t")
		yield movie_id, 1


	def reduce_count_ratings(self, key, values):	
		a=sum(values)
		yield None, ( a, key)

	def sort(self,_, map):
		for a, key  in sorted(map, reverse=False):
			yield (key, str(a).zfill(5))

if __name__=='__main__':
	lab3_2.run();
