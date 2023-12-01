from queue import Queue

class Instruction:
    def __init__ (self, transaction_id, item, type):
        self.transaction_id = transaction_id # int, e.g: 1 for T1
        self.item = item # data item, e.g: item A in R(A)
        self.type = type # read, write, commit, abort

    def __init__(self, input_sequence):
        # input_sequence: Rx(A) / Wx(A) / Cx
        if (input_sequence[0]=='R'):
            self.type = 'read'
            self.transaction_id = input_sequence[1]
            self.item = input_sequence[3]
        elif (input_sequence[0]=='W'):
            self.type = 'write'
            self.transaction_id = input_sequence[1]
            self.item = input_sequence[3]
        elif (input_sequence[0]=='C'):
            self.type = 'commit'
            self.transaction_id = input_sequence[1]
            self.item = ''
        
    def __str__(self):
        result = ''
        if (self.type=='read'):
            result += (f"R{self.transaction_id}({self.item})")
        elif (self.type=='write'):
            result += (f"W{self.transaction_id}({self.item})")
        elif (self.type=='commit'):
            result += (f"C{self.transaction_id}")
        return result

class Schedule:
    def __init__ (self):
        self.instructions = Queue() # list (queue) of instructions
        self.rollbacked_list = [] # list of transactions rollbacked
        self.logging = Queue() # list (queue) of final schedule
        
    def __init__ (self, input_sequence):
        self.instructions = Queue() # list (queue) of instructions
        self.rollbacked_list = [] # list of transactions rollbacked
        self.logging = Queue() # list (queue) of final schedule
        # input_sequence: Rx(A);Wx(A);Cx;Rx(B);Cx
        input_list = input_sequence.split(";")
        for input in input_list:
            input_instruction = Instruction(input)
            self.instructions.put(input_instruction)

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