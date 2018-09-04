from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol

class MIT805MapReduceTaxi(MRJob):    
    SORT_VALUES = True    
    OUTPUT_PROTOCOL = TextProtocol

    def steps(self):        
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)        
            ]

    def mapper(self, _, line):        
        pass

    def reducer(self, key, values):
        pass
        

if __name__ == '__main__':
    MIT805MapReduceTaxi.run()

