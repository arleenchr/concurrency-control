from queue import Queue
from collections import deque
from transaction import Transaction
from transaction_occ import TransactionOCC
from instruction import Instruction
from typing import List


class Schedule:        
    def __init__ (self, input_sequence:str, type:str='base'):
        self.current_timestamp = 0
        self.instructions = Queue() # list (queue) of instructions
        self.rollbacked_list = Queue() # list of transactions rollbacked
        self.logging = Queue() # list (queue) of final schedule
        self.type = type
        # input_sequence: Rx(A);Wx(A);Cx;Rx(B);Cx
        input_list = input_sequence.split(";")
        temp_transactions = {}
        for input in input_list:
            input_instruction = Instruction(input)
            self.instructions.put(input_instruction)
            if(input_instruction.transaction_id not in temp_transactions.keys()):
                temp_transactions.update({input_instruction.transaction_id: []})
            temp_transactions[input_instruction.transaction_id].append(input_instruction)

        self.transactions = {}
        for tid, tins in temp_transactions.items():
            if(type == 'base'):
                transaction = Transaction(tid, tins)
            elif(type == 'occ'):
                transaction = TransactionOCC(tid, tins)
            # self.current_timestamp += 1
            self.transactions.update({tid:transaction})

    
    def add_transaction(self, transaction: Transaction) -> None:
        self.transactions.append(transaction)

    def add_instruction(self, instruction):
        self.instructions.put(instruction)

    def rollback(self, transaction_id):
        self.rollbacked_list.put(transaction_id)
        logged = []
        for log in list(self.logging.queue):
            if log.transaction_id != transaction_id:
                logged.append(log)
        self.logging = Queue()
        self.logging.queue = deque(logged)



    def execute_instruction(self):
        if(self.instructions.empty() and not self.rollbacked_list.empty()):
            transaction = self.transactions.get(self.rollbacked_list.get())
            print(f'Restart T{transaction.id}')
            if(self.type=='occ'):
                transaction = TransactionOCC(transaction.id, transaction.instructions)
            for e in transaction.instructions:
                self.instructions.put(e)

        instruction = self.instructions.get()
        if (instruction.transaction_id in self.rollbacked_list.queue):
            return self.execute_instruction()
        
        print(f'{self.current_timestamp}: {instruction}')

        self.current_timestamp += 1
        self.logging.put(instruction)
        return instruction
    
    def get_logged_instructions(self, transaction_id):
        logged = [] 
        for instruction in list(self.logging.queue):
            if (instruction.transaction_id == transaction_id):
                logged.append(instruction)
        return logged
    
    def get_instructions(self, transaction_id):
        instructions = []
        for ins in list(self.instructions.queue):
            if (ins.transaction_id == transaction_id):
                instructions.append(ins)
        return instructions
    
    def remove_instructions(self, transaction_id):
        for _ in range(len(self.instructions.queue)):
            ins = self.instructions.get()
            if (ins.transaction_id != transaction_id):
                self.instructions.put(ins)
    
    def restore_instruction(self, instruction):
        # put instruction to the first element of self.instructions
        self.add_instruction(instruction)
        for _ in range(self.instructions.qsize()-1):
            curr = self.instructions.get()
            self.add_instruction(curr)


if __name__ == '__main__':
    schedule = Schedule('R1(A);R2(A)', 'occ')
    schedule.rollback('1')
    x = schedule.execute_instruction()
    y = schedule.execute_instruction()
