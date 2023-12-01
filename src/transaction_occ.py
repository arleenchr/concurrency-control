from transaction import Transaction
from typing import List
from instruction import Instruction
from enums import TransactionValidationBasedPhase, InstructionType
import time


class TransactionOCC(Transaction):
    def __init__(self, id: str, instructions: List[Instruction]) -> None:
        super().__init__(id, instructions)
        self.start_ts:float = time.time()
        self.validation_ts:float = None
        self.finish_ts:float = None
        self.data_items_written = set()
        self.data_items_read = set()
        for instruction in self.instructions:
            if instruction.type == 'write':
                self.data_items_written.add(instruction.item)
            elif instruction.type == 'read':
                self.data_items_read.add(instruction.item)

    
    def validate(self, transactions) -> bool:
        self.validation_ts = time.time()
        for ti in transactions.values():
            if ti.id == self.id:
                continue
            if ti.validation_ts and ti.validation_ts < self.validation_ts:
                # TS(Ti) < TS(Tj)
                if not(ti.finish_ts and ti.finish_ts < self.start_ts):
                    return False
                if not (ti.finish_ts and self.start_ts < ti.finish_ts < self.validation_ts and len(ti.data_items_written.intersection(self.data_items_read))==0):
                    return False
        return True


    def commit() -> None:
        self.finish_ts = time.time()


if __name__ == '__main__':
    ia = Instruction('R1(A)')
    ib = Instruction('R2(A)')
    tc = TransactionOCC('1', [ia, ib])
    print(tc.data_items_read)