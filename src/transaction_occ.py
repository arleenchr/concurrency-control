from transaction import Transaction
from typing import List
from instruction import Instruction
from enums import TransactionValidationBasedPhase, InstructionType


class TransactionOCC(Transaction):
    def __init__(self, start_ts: int, instructions: List[Instruction]) -> None:
        super().__init__(start_ts, instructions)
        self.start_ts = start_ts
        self.validation_ts = None
        self.finish_ts = None
        self.data_items_written = set()
        self.data_items.read = set()
        for instruction in self.instructions:
            if instruction.type == 'write':
                self.data_items_written.add(instruction.item)
            elif instruction.type == 'read':
                self.data_items_read.add(instruction.item)

    
    def validate(transactions) -> bool:
        for tj in transactions:
            if self.validation_ts >= tj.validation_ts:
                return False
            # TS(Ti) < TS(Tj)
            elif self.finish_ts < tj.start_ts or tj.start_ts < self.finish_ts < tj.validation_ts and len(self.data_items_written.intersection(tj.data_items_read))==0:
                return True


if __name__ == '__main__':
    ia = Instruction(1, 'A', 'write')
    ib = Instruction(2, 'A', 'read')
    tc = TransactionOCC([ia, ib], 0)
    print(tc.data_items_written)