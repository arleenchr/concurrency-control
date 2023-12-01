from collections import deque
import re
from schedule import *

class TwoPhaseLock:
    def __init__(self, input_sequence):
        self.schedule = Schedule(input_sequence)
        self.locks = {} # {'instruction': instruction, 'locktype': x_lock/s_lock}
        self.instruction_queue = deque([])
        self.waiting_list = {} # {'transaction_id': [list of transactions waiting for the transaction]}
        for ins in list(self.schedule.instructions.queue):
            if (ins.transaction_id not in self.waiting_list):
                self.waiting_list[ins.transaction_id] = []
        self.executed = []
        
    def add_schedule_instruction(self, instruction):
        # apend instruction to schedule
        self.schedule.add_instruction(instruction)

    def execute_instruction(self):
        # execute current instruction
        return self.schedule.execute_instruction()
    
    def check_lock_avail(self, lock_type, item, transaction_id):
        # check if item can acquire x_lock on transaction_id
        # return true/false and transaction it waits for (-1 if true)
        for ins,lock in self.locks.items():
            if (lock_type == 'x_lock'):
                if (ins.item==item and ins.transaction_id!=transaction_id):
                    # if there's another transaction that holds a lock on the item, then x_lock is not available
                    return False, ins.transaction_id
            else: # s_lock
                if (lock=='x_lock' and ins.item==item and ins.transaction_id!=transaction_id):
                    # if there's another transaction that holds an x_lock on the item, then s_lock is not available
                    return False, ins.transaction_id
        return True, -1

    def acquire_lock(self, instruction):
        if (instruction.type=='read'):
            is_s_lock_avail = self.check_lock_avail('s_lock', instruction.item, instruction.transaction_id)
            if (not is_s_lock_avail[0]):
                # lock not avail, add to queue and waiting list
                self.instruction_queue.append(instruction)
                if (instruction.transaction_id not in self.waiting_list[is_s_lock_avail[1]]):
                    self.waiting_list[is_s_lock_avail[1]].append(instruction.transaction_id)
                return False
            # else, acquire lock
            self.locks[instruction] = 's_lock'

        elif (instruction.type=='write'):
            is_x_lock_avail = self.check_lock_avail('x_lock', instruction.item, instruction.transaction_id)
            if (not is_x_lock_avail[0]):
                # lock not avail, add to queue and waiting list
                self.instruction_queue.append(instruction)
                if (instruction.transaction_id not in self.waiting_list[is_x_lock_avail[1]]):
                    self.waiting_list[is_x_lock_avail[1]].append(instruction.transaction_id)
                return False
            # else, acquire lock
            self.locks[instruction] = 'x_lock'
            # automatic upgrade lock from s_lock to x_lock
            for ins,lock in self.locks.items():
                if (lock=='s_lock' and ins.transaction_id==instruction.transaction_id and ins.item==instruction.item):
                    self.release_lock(ins)
                    break
        return True

    def release_lock(self, instruction):
        # release lock
        self.locks.pop(instruction)

    def commit(self, instruction):
        # commit, if not available then add to queue
        if (not self.check_commit_avail(instruction.transaction_id)):
            self.add_to_queue(instruction)
            return False
        return True

    def check_commit_avail(self, transaction_id):
        # check if commit can be executed
        # commit can not be executed if there are instructions within the same transaction waiting in the queue
        for ins in self.instruction_queue:
            if (ins.transaction_id == transaction_id):
                return False
        return True

    def add_to_queue(self, instruction):
        self.instruction_queue.append(instruction)
    
    def search_queue(self, transaction_id):
        # search if there's any instructions within transaction_id waiting in the queue
        for ins in self.instruction_queue:
            if (ins.transaction_id == transaction_id):
                return True
        return False
    
    def remove_from_queue(self, transaction_id):
        # remove all instructions within transaction_id from the queue
        for _ in range(len(self.instruction_queue)):
            ins = self.instruction_queue.popleft()
            if (ins.transaction_id != transaction_id):
                self.add_to_queue(ins)

    def execute_queue(self):
        # execute instructions waiting in the queue
        instruction = self.instruction_queue.popleft()
        if (instruction.type=='read' or instruction.type=='write'):
            is_lock_acquired = self.acquire_lock(instruction)
            if (is_lock_acquired):
                print(f"Current instruction: {instruction}")
                print(f"Action\t\t:acquire {self.locks[instruction]} for {instruction.item} on T{instruction.transaction_id}")
                self.executed.append(instruction)
            return is_lock_acquired, instruction
            
        elif (instruction.type=='commit'):
            is_commit = self.commit(instruction)
            if (is_commit):
                print(f"Current instruction: {instruction}")
                commit_instructions = self.get_executed_instructions(instruction.transaction_id)
                cnt = 0
                print("Action\t\t:", end='')
                for ins in commit_instructions:
                    if (ins.type!='commit'):
                        if (ins in self.locks):
                            print('\t\t' if cnt>0 else '', end='')
                            print(f"release {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                            self.release_lock(ins)
                            cnt+=1 
                self.executed.append(instruction)
            return is_commit, instruction

    def abort(self, transaction_id):
        # abort mechanism
        instructions = self.schedule.get_logged_instructions(transaction_id)
        for ins in instructions:
            if (ins.type!='commit'):
                # release locks
                cnt = 0
                if (ins in self.locks):
                    print(f"\trelease {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                    self.release_lock(ins)
                    cnt+=1 
            # remove instructions from executed instructions list
            if (ins in self.executed):
                self.executed.remove(ins)
        # remove waiting instructions from the queue
        self.remove_from_queue(transaction_id)
        # adding all instructions within the transactions to the queue
        transaction = self.schedule.get_logged_instructions(transaction_id) # executed instructions
        [self.add_to_queue(ins) for ins in transaction]
        upcoming_ins = self.schedule.get_instructions(transaction_id) # upcoming instructions
        [self.add_to_queue(ins) for ins in upcoming_ins]
        self.schedule.remove_instructions(transaction_id)
        # clear the waiting list
        self.waiting_list[transaction_id] = []
    
    def get_executed_instructions(self, transaction_id):
        # returns all executed instructions within transaction_id
        result = []
        for ins in self.executed:
            if (ins.transaction_id == transaction_id):
                result.append(ins)
        return result

    def print_queue(self):
        print('Queue\t\t: [', end='')
        cnt = 0
        for ins in self.instruction_queue:
            print(ins, end='')
            print(', ' if cnt<len(self.instruction_queue)-1 else '', end='')
            cnt+=1
        print(']')

    def print_locks(self):
        print('Locks\t\t: [', end='')
        cnt = 0
        for ins,lock in self.locks.items():
            print(f"{lock}{ins.transaction_id}({ins.item})", end='')
            print(', ' if cnt<len(self.locks)-1 else '', end='')
            cnt+=1
        print(']')

    def print_waiting_list(self):
        print('Waiting list\t: [', end='')
        cnt = 0
        for waited,transaction_list in self.waiting_list.items():
            if (len(transaction_list)>0):
                print(f"T{waited} is waited by ", end='')
                print(', '.join([f"T{i}" for i in transaction_list]), end=';')
        print(']')

    def print_executed(self):
        print('FINAL SCHEDULE\t: ', end='')
        cnt = 0
        for ins in self.executed:
            print(ins, end=';')
            cnt+=1
        print()

    def run(self):
        while (not self.schedule.instructions.empty() or len(self.instruction_queue)>0):
            if (not self.schedule.instructions.empty()):
                current_instruction = self.execute_instruction()
                print(f"Current instruction: {current_instruction}")
                
                # if there's any instructions within the same transaction waiting in queue, then the current instruction also added to the queue (waiting)
                if (self.search_queue(current_instruction.transaction_id)):
                    self.add_to_queue(current_instruction)
                    print("Action\t\t:Waiting. Instruction added to queue")
                    self.print_queue()
                    self.print_locks()
                    self.print_waiting_list()
                    self.print_executed()
                    print("==================================================================")
                else:
                    if (current_instruction.type=='read' or current_instruction.type=='write'):
                        is_lock_acquired = self.acquire_lock(current_instruction)
                        if (not is_lock_acquired):
                            print(f"Action\t\t:{'s_lock' if current_instruction.type=='read' else 'x_lock'} not granted. Instruction added to queue")
                            if (len(self.waiting_list[current_instruction.transaction_id])>0):
                                # if there's any transaction waiting for this transaction
                                for t_id in self.waiting_list[current_instruction.transaction_id]:
                                    print(f"Action\t\t:Abort T{t_id}")
                                    self.abort(t_id)
                                    current_wound = self.instruction_queue.popleft()
                                    self.schedule.restore_instruction(current_wound)
                                    
                        else:
                            print(f"Action\t\t:acquire {self.locks[current_instruction]} for {current_instruction.item} on T{current_instruction.transaction_id}")
                            self.executed.append(current_instruction)
                        self.print_queue()
                        self.print_locks()
                        self.print_waiting_list()
                        self.print_executed()
                        print("==================================================================")

                    elif (current_instruction.type=='commit'):
                        is_commit = self.commit(current_instruction)
                        if (is_commit):
                            commit_instructions = self.schedule.get_logged_instructions(current_instruction.transaction_id)
                            cnt = 0
                            for ins in commit_instructions:
                                if (ins.type!='commit'):
                                    # release locks
                                    if (ins in self.locks):
                                        print(f"Action\t\t:release {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                                        self.release_lock(ins)
                                cnt+=1 
                            # clear the waiting list
                            self.waiting_list[current_instruction.transaction_id] = []

                            self.executed.append(current_instruction)
                            self.print_queue()
                            self.print_locks()
                            self.print_waiting_list()
                            self.print_executed()
                            print("==================================================================")
                            # if commited, then execute all queue
                            for _ in range (len(self.instruction_queue)):
                                exec_queue = self.execute_queue()
                                # print(f"Current instruction: {exec_queue[1]}")
                                self.print_queue()
                                self.print_locks()
                                self.print_waiting_list()
                                self.print_executed()
                                print("==================================================================")
                        else:
                            print("Action\t\t:Not executed. Instruction added to queue")

        self.print_executed()

if __name__ == '__main__':
    print('Two Phase Locking')
    print("==================================================================")
    
    # receive input
    is_valid = False
    while (not is_valid):
        print("Input your schedule:")
        sequence = input()
        print("==================================================================")
        is_valid = re.search("^(?:[RW]\d+\([A-Z]\)|C\d+)(?:;(?:[RW]\d+\([A-Z]\)|C\d+))*$", sequence)

    locking = TwoPhaseLock(sequence)

    locking.run()