from queue import Queue

class Instruction:
    def __init__ (self, transaction_id, item, type):
        self.transaction_id = transaction_id # int, e.g: 1 for T1
        self.item = item # data item, e.g: item A in R(A)
        self.type = type # read, write, commit, abort

class Schedule:
    def __init__ (self):
        self.instructions = Queue() # list (queue) of instructions
        self.rollbacked_list = [] # list of transactions rollbacked
        self.logging = Queue() # list (queue) of final schedule
        
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
    
    