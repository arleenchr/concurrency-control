from queue import Queue
from transaction import Transaction
from transaction_occ import TransactionOCC
from instruction import Instruction
from typing import List


class Schedule:        
    def __init__ (self, input_sequence:str, type:str='base'):
        self.instructions = Queue() # list (queue) of instructions
        self.rollbacked_list = [] # list of transactions rollbacked
        self.logging = Queue() # list (queue) of final schedule
        # input_sequence: Rx(A);Wx(A);Cx;Rx(B);Cx
        input_list = input_sequence.split(";")
        temp_transactions = {}
        for input in input_list:
            input_instruction = Instruction(input)
            self.instructions.put(input_instruction)
            if(input_instruction.transaction_id not in temp_transactions.keys()):
                temp_transactions.update({input_instruction.transaction_id: []})
            temp_transactions[input_instruction.transaction_id].append(input_instruction)

        self.transactions = []
        for tid, tins in temp_transactions.items():
            if(type == 'base'):
                transaction = Transaction(tid, tins)
            elif(type == 'occ'):
                transaction = TransactionOCC(tid, tins)
            self.transactions.append(transaction)

    
    def add_transaction(self, transaction: Transaction) -> None:
        self.transactions.append(transaction)

    def add_instruction(self, instruction):
        self.instructions.put(instruction)

    def rollback(self, transaction_id):
        self.rollbacked_list.append(transaction_id)


    def execute_instruction(self):
        instruction = self.instructions.get()

        if (instruction.transaction_id in self.rollbacked_list):
            self.instructions.put(instruction)
            return self.execute_instruction()
        
        self.logging.put(instruction)
        return instruction
    
    def get_logged_instructions(self, transaction_id):
        logged = [] 
        for instruction in list(self.logging.queue):
            if (instruction.transaction_id == transaction_id):
                logged.append(instruction)
        return logged


if __name__ == '__main__':
    schedule = Schedule('R1(A);R2(A)', 'occ')
    print(schedule.transactions[0].data_items_read)