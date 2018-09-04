from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol

'''
    Counts the  vendor_id
'''
class MIT805MapReduceTaxi_CountVendors(MRJob):
    SORT_VALUES = True    
    OUTPUT_PROTOCOL = TextProtocol

    def steps(self):        
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)        
            ]

    def mapper(self, _, line):
        entries = line.split(',')
        # used to ignore the first line which is the header of the file
        if entries[2].strip() != "vendor_id":
            yield entries[2], 1
        pass

    def reducer(self, key, values):
        array = [x for x in values]
        yield key, str(sum(array))
'''
 Calculates the Average  tip_amount 
'''
class MIT805MapReduceTaxi_AverageTip(MRJob):
    SORT_VALUES = True
    OUTPUT_PROTOCOL = TextProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
            ]

    def mapper(self, _, line):
        entries = line.split(',')
        # used to ignore the first line which is the header of the file
        if entries[2].strip() != "vendor_id":
            # Tip Amount
            yield "Tip", entries[8]
        pass

    def reducer(self, key, values):
        i, totalL, totalW = 0.0, 0.0, 0.0
        for i in values:
            totalL += 1
            totalW += float(i)
        yield "Average", str(totalW / float(totalL))

'''
 Calculates the Max & Min tip_amount 
'''
class MIT805MapReduceTaxi_MaxMinTip(MRJob):
    SORT_VALUES = True
    OUTPUT_PROTOCOL = TextProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
            ]

    def mapper(self, _, line):
        entries = line.split(',')
        # used to ignore the first line which is the header of the file
        if entries[2].strip() != "vendor_id":
            # Tip Amount
            yield "Tip", entries[8]
        pass

    def reducer(self, key, values):
        min = 9999.99
        max = 0
        # Finds the min&max Tip value in the array of values
        for i in values:
            value = float(i)
            if value > max:
                max = value
            if value < min:
                min = value

        yield "Max Tip: ", str(max)
        yield "Min Tip: ", str(min)

if __name__ == '__main__':

    menu = "\n *** MIT805 Assignment 2 Part 2 ***" \
           "\n 1. Count Vendors" \
           "\n 2. Average TIP" \
           "\n 3. Max and Min TIP" \
           "\n q. QUIT"

    while True:
        print(menu)
        user_input = input("Choice: ")

        if user_input == "1":
            MIT805MapReduceTaxi_CountVendors.run()
        elif user_input == "2":
            MIT805MapReduceTaxi_AverageTip.run()
        elif user_input == "3":
            MIT805MapReduceTaxi_MaxMinTip.run()
        elif user_input == "q":
            break
        else:
            continue


