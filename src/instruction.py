from enums import InstructionType


class Instruction:
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
