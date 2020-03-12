import pickle
import random
import os
import sys

# Operates with a 3-tiered check for allocated ints in descending speed order
#
# Ints will be either in class instance's personal in-mem allocation set OR on disk in deep storage,
# but Counting Bloom Filter acts as a first pass to be sure a number is definitely not yet allocated
# while still allowing for removals
#
# With extra time, I'd also keep track of numbers NOT yet allocated by creating a mirrored system for
# not-yet-allocated ints so that we can have efficient checks and lookups as the balance shifts from
# mostly empty to mostly full--currently the system is optimized for sparse allocations/releases rather than
# heavy storage near capacity and should be applied to appropriate use cases.

class IntegerSet():
    def __init__(self):
        self.__num_filter_alloc = [0 for i in range(0,len(bin(hash("{0:b}".format(1)))))]
        self.__allocation_set = set()
        self.__total = 0

    def _dump_set_to_disk(self)->None:
        if os.path.exists("serialized_numset.pkl"):
            f = open("serialized_numset.pkl", 'wb')
            existing_set:set = pickle.load(f)
            existing_set.union(self.__allocation_set)
            pickle.dump(existing_set, f)
        else:
            f = open("serialized_numset.pkl", 'wb')
            pickle.dump(self.__allocation_set, f)
        self.__allocation_set = set()
        f.close()

    # Check OR remove from disk; adding taken care of in allocate f(x)
    def _check_disk(self, num:int, remove:bool=False)->bool:
        if os.path.exists("serialized_numset.pkl"):
            f = open("serialized_numset.pkl", 'wb')
            disk_set:set = pickle.load(f)
            if int in disk_set:
                if remove:
                    disk_set.remove(num)
                pickle.dump(disk_set,f)
                f.close()
                return True
        return False

    # Achilles' Heel: Check rand slows down miserably as we approach capacity; could solve by keeping track of non-allocated
    # ints separately, storing mostly on disk in random order, and adding/removing in batches
    def allocate(self)->int:
        for i in range(10000000):
            rando = random.randrange(10000000)
            if not self._check_filter(rando) or \
                    rando not in self.__allocation_set or\
                    not self._check_disk(rando):
                self._add_to_filter(rando)
                self.__allocation_set.add(rando)
                if len(self.__allocation_set) > 1000000:
                    self._dump_set_to_disk()
                self.__total +=1
                return rando
            if i % 1000000 and self.__total == 10000000: # Primitive means of checking for max when dealing with Bloom Filter
                return 0

    def release(self, x: int) -> bool:
        alloc_set_member = x in self.__allocation_set
        if not self._check_filter(x) and \
                not alloc_set_member and \
                not self._check_disk(x):
            return False
        else:
            self._rm_from_filter(x)
            if alloc_set_member:
                self.__allocation_set.remove(x)
            else:
                self._check_disk(x, remove=True)
            self.__total -= 1
            return True

    def _check_filter(self, num:int)->bool: # maybe true or DEF NOT true
        ha = self._fancy_hash(num)
        for i, bit in enumerate(ha):
            if bit == '1':
                if self.__num_filter_alloc[i] != 1:
                    return False
        return True

    def _add_to_filter(self, num:int)->None:
        ha = self._fancy_hash(num)
        for ind,bit in enumerate(ha):
           if bit == '1':
               self.__num_filter_alloc[ind] += 1

    def _rm_from_filter(self, num:int)->None:
        ha = self._fancy_hash(num)
        for ind, bit in enumerate(ha):
            if bit == '1':
                self.__num_filter_alloc[ind] -= 1

    def _fancy_hash(self, num:int)->str:
        # maxsize just makes hash positive
        return "{0:b}".format(hash("{0:b}".format(num))% ((sys.maxsize + 1) * 2))


if __name__ == "__main__":
    num_vault = IntegerSet()

    test_nums = [num_vault.allocate() for i in range(100)]

    print('Test nums allocated: ')
    print(test_nums)

    print('De-allocate junk test')
    false_test = [i for i in range(200) if i not in test_nums]
    print(num_vault.release(false_test[0]))

    print('Deallocate valid nums:')
    print(num_vault.release(test_nums[0]))
    print("Deallocate already deallocated num:")
    print(num_vault.release(test_nums[0]))
    print('')
