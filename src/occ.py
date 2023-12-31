from schedule import Schedule
from collections import deque
from queue import Queue
from instruction import Instruction
from transaction_occ import TransactionOCC
import re

class OCC:
    def __init__(self, input_sequence: str) -> None:
        self.schedule: Schedule = Schedule(input_sequence, type='occ')


    def _transform_schedule(self) -> None:
        instructions = list(self.schedule.instructions.queue)
        writes = {}
        for ins in instructions:
            if ins.type == 'write':
                if(ins.transaction_id not in writes.keys()):
                    writes.update({ins.transaction_id: []})
                writes[ins.transaction_id].append(ins)
        
        for tid,arr in writes.items():
            for el in arr:
                instructions.remove(el)
        for tid, arr in writes.items():
            commit_idx = len(instructions)-1
            for i in range(len(instructions)):
                ins = instructions[i]
                if ins.type == 'commit' and ins.transaction_id==tid:
                    commit_idx = i
                    break
            instructions[:commit_idx] += arr

        self.schedule.instructions = Queue()
        self.schedule.instructions.queue = deque(instructions)

        temp_transactions = {}
        for ins in instructions:
            if(ins.transaction_id not in temp_transactions.keys()):
                temp_transactions.update({ins.transaction_id:[]})
            temp_transactions.get(ins.transaction_id).append(ins)
        
        for tid, tins in temp_transactions.items():
            self.schedule.transactions.update({tid:TransactionOCC(tid, tins)})



    def get_transaction(self, transaction_id):
        for tid, t in self.schedule.transactions.items():
            if tid == transaction_id:
                tj = t
        return tj


    def run(self) -> None:
        processed = set()
        while not (self.schedule.instructions.empty() and self.schedule.rollbacked_list.empty()):
            instruction = self.schedule.execute_instruction()
            tj = self.get_transaction(instruction.transaction_id)
            
            if tj.id not in processed:
                tj.start(self.schedule.current_timestamp-1)
            processed.add(tj.id)

            if instruction.type == 'write':
                valid = tj.validate(self.schedule.transactions, self.schedule.current_timestamp-1)
                if valid:
                    while not (instruction.type == 'commit' and instruction.transaction_id == tj.id):
                        instruction = self.schedule.execute_instruction()
                    tj.commit(self.schedule.current_timestamp-1)
                    print(f'[{tj.id}] startTS:{tj.start_ts}\tvalidationTS:{tj.validation_ts}\tfinishTS:{tj.finish_ts}')
                else:
                    print(f'Rollback: T{tj.id}')
                    self.schedule.rollback(tj.id)
                    processed.remove(tj.id)
            elif instruction.type == 'commit':
                valid = tj.validate(self.schedule.transactions, self.schedule.current_timestamp-1)
                if valid:
                    tj.commit(self.schedule.current_timestamp-1)
                else:
                    print(f'Rollback: T{tj.id}')
                    self.schedule.rollback(tj.id)
                    processed.remove(tj.id)
                print(f'[{tj.id}] startTS:{tj.start_ts}\tvalidationTS:{tj.validation_ts}\tfinishTS:{tj.finish_ts}')

        logs = [ins.__str__() for ins in self.schedule.logging.queue]
        print(';'.join(logs))

        # print('===!!===')
        # for tj in self.schedule.transactions.values():
            # print(f'[{tj.id}] startTS:{tj.start_ts}\tvalidationTS:{tj.validation_ts}\tfinishTS:{tj.finish_ts}')



if __name__ == '__main__':
    print('Optimistic Concurrency Control (Validation Based Protocol)')
    print('=================')
    
    # receive input
    is_valid = False
    while (not is_valid):
        print("Input your schedule:")
        sequence = input()
        print("==================================================================")
        is_valid = re.search("^(?:[RW]\d+\([A-Z]\)|C\d+)(?:;(?:[RW]\d+\([A-Z]\)|C\d+))*$", sequence)

    occ = OCC(sequence)
    occ.run()
    # sequence = 'R1(b);R2(b);R2(a);W2(b);W2(a);R1(a);C1;C2'
    # sequence = 'R1(A);R2(A);W1(A);C1;C2'
    # sequence = 'R1(A);W2(A);W2(B);W3(B);W1(A);C1;C2;C3'
    # sequence = 'R2(A);R1(A);W1(A);R2(B);W2(A);W1(B);C1;C2'
    # sequence = 'R1(A);R2(A);R2(B);R3(B);W3(A);W2(A);W1(B);C3;C2;C1'
    # sequence = 'R1(A);R1(B);W1(A);W3(B);R2(B);W1(C);R2(A);C1;C2;C3'
    # occ = OCC(sequence)
    # # for q in occ.schedule.instructions.queue:
    # #     print(q)
    # occ._transform_schedule()
    # for q in occ.schedule.instructions.queue:
    #     print(q)
    # print('====')
    # occ.run()
    # print('==============')

    # occ.run()


