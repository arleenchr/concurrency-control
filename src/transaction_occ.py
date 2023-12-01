from transaction import Transaction
from typing import List
from instruction import Instruction
from enums import TransactionValidationBasedPhase, InstructionType
# from time import perf_counter_ns


class TransactionOCC(Transaction):
    def __init__(self, id: str, instructions: List[Instruction]) -> None:
        super().__init__(id, instructions)
        self.start_ts:int = None
        self.validation_ts:int = None
        self.finish_ts:int = None
        self.data_items_written = set()
        self.data_items_read = set()
        for instruction in self.instructions:
            if instruction.type == 'write':
                self.data_items_written.add(instruction.item)
            elif instruction.type == 'read':
                self.data_items_read.add(instruction.item)


    def start(self, current_timestamp) -> None:
        self.start_ts = current_timestamp

    
    def validate(self, transactions, current_timestamp) -> bool:
        self.validation_ts = current_timestamp
        res = True
        for ti in transactions.values():
            if ti.id == self.id:
                continue
            if ti.validation_ts and ti.validation_ts < self.validation_ts:
                # TS(Ti) < TS(Tj)
                if ti.finish_ts and ti.finish_ts < self.start_ts:
                    # print('gala')
                    pass
                    # return False
                elif ti.finish_ts and self.start_ts < ti.finish_ts < self.validation_ts and len(ti.data_items_written.intersection(self.data_items_read))==0:
                    # print('galb')
                    pass
                    # return False
                else:
                    return False
        return True


    def commit(self, current_timestamp) -> None:
        self.finish_ts = current_timestamp


if __name__ == '__main__':
    ia = Instruction('R1(A)')
    ib = Instruction('R2(A)')
    tc = TransactionOCC('1', [ia, ib])
    print(tc.data_items_read)