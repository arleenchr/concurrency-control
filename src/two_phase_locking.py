from collections import deque
import re
from schedule import *

class TwoPhaseLock:
    def __init__(self):
        self.schedule = Schedule()
        self.locks = {} # {'instruction': instruction, 'locktype': x_lock/s_lock}
        self.instruction_queue = deque([])
        # self.sequence = deque([]) # {"type": read/write/commit/aborted/tempwrite, "tx": transaction_number, "item": data_item}

    def __init__(self, input_sequence):
        self.schedule = Schedule(input_sequence)
        self.locks = {} # {'instruction': instruction, 'locktype': x_lock/s_lock}
        self.instruction_queue = deque([])
        # self.sequence = deque([]) # {"type": read/write/commit/aborted/tempwrite, "tx": transaction_number, "item": data_item}

    def add_schedule_instruction(self, instruction):
        self.schedule.add_instruction(instruction)

    def execute_instruction(self):
        return self.schedule.execute_instruction()
    
    def check_lock_avail(self, lock_type, item, transaction_id):
        # check if item can acquire x_lock on transaction_id
        for ins,lock in self.locks.items():
            # print(ins)
            if (lock_type == 'x_lock'):
                if (ins.item==item and ins.transaction_id!=transaction_id):
                    # if there's another transaction that holds a lock on the item, then x_lock is not available
                    return False
            else: # s_lock
                if (lock=='x_lock' and ins.item==item and ins.transaction_id!=transaction_id):
                    # if there's another transaction that holds an x_lock on the item, then s_lock is not available
                    return False
        return True

    def acquire_lock(self, instruction):
        # self.print_locks()
        # print(f"{instruction.type}\t{instruction.item}\tT{instruction.transaction_id}\t", end='')
        # print(f"Current instruction: {instruction}")
        
        if (instruction.type=='read'):
            if (not self.check_lock_avail('s_lock', instruction.item, instruction.transaction_id)):
                # lock not avail
                self.instruction_queue.appendleft(instruction)
                # print(f"s_lock not granted.")
                # self.print_queue()
                return False
            self.locks[instruction] = 's_lock'

        elif (instruction.type=='write'):
            if (not self.check_lock_avail('x_lock', instruction.item, instruction.transaction_id)):
                # lock not avail
                self.instruction_queue.appendleft(instruction)
                # print(f"x_lock not granted.")
                # self.print_queue()
                return False
            self.locks[instruction] = 'x_lock'

        # print(f"acquire {self.locks[instruction]} for {instruction.item} on T{instruction.transaction_id}")
        return True

    def release_lock(self, instruction):
        self.locks.pop(instruction)

    def commit(self, instruction):
        # check queue dulu
        # print(f"Current instruction: {instruction}\nAction: ", end='')
        if (not self.check_commit_avail(instruction.transaction_id)):
            self.add_to_queue(instruction)
            return False
        
        return True

    def check_commit_avail(self, transaction_id):
        for ins in self.instruction_queue:
            if (ins.transaction_id == transaction_id):
                return False
        return True

    def add_to_queue(self, instruction):
        self.instruction_queue.append(instruction)

    def execute_queue(self):
        instruction = self.instruction_queue.popleft()
        # print(instruction)
        if (instruction.type=='read' or instruction.type=='write'):
            is_lock_acquired = self.acquire_lock(instruction)
            # return self.acquire_lock(instruction)
            if (is_lock_acquired):
                print(f"Current instruction: {instruction}")
                print(f"Action: acquire {self.locks[instruction]} for {instruction.item} on T{instruction.transaction_id}")
            return is_lock_acquired
            #     self.instruction_queue.appendleft(instruction)
        elif (instruction.type=='commit'):
            is_commit = self.commit(instruction)
            if (is_commit):
                print(f"Current instruction: {instruction}")
                commit_instructions = self.schedule.get_logged_instructions(instruction.transaction_id)
                cnt = 0
                print("Action: ", end='')
                for ins in commit_instructions:
                    if (ins.type!='commit'):
                        print('\t' if cnt>0 else '', end='')
                        print(f"release {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                        self.release_lock(ins)
                    cnt+=1 
            return is_commit
            # if (not self.commit(instruction)):
            #     self.instruction_queue.appendleft(instruction)

    def print_queue(self):
        print('Queue: [', end='')
        cnt = 0
        for ins in self.instruction_queue:
            print(ins, end='')
            print(', ' if cnt<len(self.instruction_queue)-1 else '', end='')
            cnt+=1
        print(']')

    def print_locks(self):
        print('Locks: [', end='')
        cnt = 0
        for ins,lock in self.locks.items():
            print(f"{lock}{ins.transaction_id}({ins.item})", end='')
            print(', ' if cnt<len(self.locks)-1 else '', end='')
            cnt+=1
        print(']')

    # def rollback(self):

    # def print_sequence(self):

    def run(self):
        # print("\nType\tItem\tTx\tCCM")
        while (not self.schedule.instructions.empty() or len(self.instruction_queue)>0):
            is_queue_exec = False
            print("==============")
            if (len(self.instruction_queue) > 0):
                is_queue_exec = self.execute_queue()
                # if (is_queue_exec):
                #     print("Action: ", end='')
            if (not is_queue_exec and not self.schedule.instructions.empty()):
                # print("ngga exec queue bng")
                current_instruction = self.execute_instruction()
                print(f"Current instruction: {current_instruction}")
                # print("Action: ", end='')
                if (current_instruction.type=='read' or current_instruction.type=='write'):
                    is_lock_acquired = self.acquire_lock(current_instruction)
                    if (not is_lock_acquired):
                        print(f"Action: {'s_lock' if current_instruction.type=='read' else 'x_lock'} not granted. Instruction added to queue")
                    else:
                        print(f"Action: acquire {self.locks[current_instruction]} for {current_instruction.item} on T{current_instruction.transaction_id}")
                elif (current_instruction.type=='commit'):
                    is_commit = self.commit(current_instruction)
                    if (is_commit):
                        commit_instructions = self.schedule.get_logged_instructions(current_instruction.transaction_id)
                        cnt = 0
                        # print("Action: ", end='')
                        for ins in commit_instructions:
                            if (ins.type!='commit'):
                                print(f"Action: release {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                                self.release_lock(ins)
                            cnt+=1 
                    else:
                        print("Action: Not executed. Instruction added to queue")
            self.print_queue()
            self.print_locks()

            # print(self.schedule.rollbacked_list)
            # print(f"{current_instruction.type}\t{current_instruction.item}\tT{current_instruction.transaction_id}\t", end='')
            # print(f"acquire {self.locks[current_instruction]} for {current_instruction.item} on {current_instruction.transaction_id}" if (current_instruction.type=='read' or current_instruction.type=='write') else "")


if __name__ == '__main__':
    print('Two Phase Locking')
    print('=================')
    print('Input your schedule:')
    sequence = input()
    # input_pattern = re.compile(r'^[RW]\d+\([A-Z]\);?(?:[C]\d+;?)*$')
    
    # while (not input_pattern.match(sequence)):
    #     sequence = input()

    locking = TwoPhaseLock(sequence)
    # for ins in list(locking.schedule.instructions.queue):
    #     print(f'{ins.type} {ins.item} in transaction {ins.transaction_id}')

    locking.run()
    # sample = Instruction("R1(A)")
    # locking.add_to_queue(sample)
    # locking.add_to_queue(sample)
    # locking.print_queue()