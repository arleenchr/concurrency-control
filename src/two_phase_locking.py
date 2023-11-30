from collections import deque
from schedule import *

class TwoPhaseLock:
    def __init__(self):
        self.schedule = Schedule()
        self.locks = {} # {'instruction': instruction, 'locktype': x_lock/s_lock}
        self.queue = deque([])
        # self.sequence = deque([]) # {"type": read/write/commit/aborted/tempwrite, "tx": transaction_number, "item": data_item}

    def __init__(self, input_sequence):
        self.schedule = Schedule(input_sequence)
        self.locks = {} # {'instruction': instruction, 'locktype': x_lock/s_lock}
        self.queue = deque([])
        # self.sequence = deque([]) # {"type": read/write/commit/aborted/tempwrite, "tx": transaction_number, "item": data_item}

    def add_schedule_instruction(self, instruction):
        self.schedule.add_instruction(instruction)

    def execute_instruction(self):
        return self.schedule.execute_instruction()

    def acquire_lock(self, instruction):
        if (instruction.type=='read' or instruction.type=='write'):
            if (instruction.type=='read'):
                self.locks[instruction] = 's_lock'
            elif (instruction.type=='write'):
                self.locks[instruction] = 'x_lock'
            print(f"{instruction.type}\t{instruction.item}\tT{instruction.transaction_id}\t", end='')
            print(f"acquire {self.locks[instruction]} for {instruction.item} on T{instruction.transaction_id}")

    def release_lock(self, instruction):
        self.locks.pop(instruction)

    def commit(self, instruction):
        commit_instructions = self.schedule.get_logged_instructions(instruction.transaction_id)

        cnt = 0
        for ins in commit_instructions:
            if (ins.type!='commit'):
                print('\t\t\t' if cnt>0 else f"{instruction.type}\t{instruction.item}\tT{instruction.transaction_id}\t", end='')
                print(f"release {self.locks[ins]} for {ins.item} on T{ins.transaction_id}")
                self.release_lock(ins)
            cnt+=1
        

    def add_to_queue(self, instruction):
        self.queue.append(instruction)

    # def rollback(self):

    # def print_sequence(self):

    def run(self):
        print("\nType\tItem\tTx\tCCM")
        while (not self.schedule.instructions.empty()):
            current_instruction = self.execute_instruction()
            if (current_instruction.type=='read' or current_instruction.type=='write'):
                self.acquire_lock(current_instruction)
            elif (current_instruction.type=='commit'):
                self.commit(current_instruction)

            # print(f"{current_instruction.type}\t{current_instruction.item}\tT{current_instruction.transaction_id}\t", end='')
            # print(f"acquire {self.locks[current_instruction]} for {current_instruction.item} on {current_instruction.transaction_id}" if (current_instruction.type=='read' or current_instruction.type=='write') else "")


if __name__ == '__main__':
    print('Two Phase Locking')
    print('=================')
    print('Input your schedule:')
    sequence = input()

    locking = TwoPhaseLock(sequence)
    # for ins in list(locking.schedule.instructions.queue):
    #     print(f'{ins.type} {ins.item} in transaction {ins.transaction_id}')

    locking.run()