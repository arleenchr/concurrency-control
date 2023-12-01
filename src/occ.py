from schedule import Schedule
from collections import deque
from queue import Queue
from instruction import Instruction

class OCC:
    def __init__(self, input_sequence: str) -> None:
        self.schedule: Schedule = Schedule(input_sequence, type='occ')


    def _transform_schedule(self) -> None:
        instructions = list(self.schedule.instructions.queue)
        writes = {}
        for ins in instructions:
            if ins.type == 'write' or ins.type == 'commit':
                if(ins.transaction_id not in writes.keys()):
                    writes.update({ins.transaction_id: []})
                writes[ins.transaction_id].append(ins)
        
        lasts = list(writes.keys())
        for i in range(len(instructions)-1, -1, -1):
            ins = instructions[i]
            if ins.type == 'read' and ins.transaction_id in lasts:
                lasts.remove(ins.transaction_id)
                instructions.insert(i+1, 'x'+ins.transaction_id)
        for i in range(len(instructions)-1, -1, -1):
            ins = instructions[i]
            if not isinstance(ins, str) and ins.type == 'write' and ins.transaction_id in lasts:
                lasts.remove(ins.transaction_id)
                instructions.insert(i+1, 'x'+ins.transaction_id)

        for tid,arr in writes.items():
            for el in arr:
                instructions.remove(el)

        for tid, arr in writes.items():
            last_idx = len(instructions)-1
            for i in range(len(instructions)):
                ins = instructions[i]
                if isinstance(ins, str) and ins[1:]==tid:
                    last_idx = i
                    break
            instructions[:last_idx] += arr

        for tid in writes.keys():
            for el in instructions:
                if el == 'x'+tid:
                    instructions.remove(el)

        self.schedule.instructions = Queue()
        self.schedule.instructions.queue = deque(instructions)


    def run(self) -> None:
        while not self.schedule.instructions.empty():
            instruction = self.schedule.execute_instruction()
            if instruction.type == 'write':
                tj = None
                for tid, t in self.schedule.transactions.items():
                    if tid == instruction.transaction_id:
                        tj = t
                
                valid = tj.validate(self.schedule.transactions)
                if valid:
                    while not (instruction.type == 'commit' and instruction.transaction_id == tj.id):
                        instruction = self.schedule.execute_instruction()
                    tj.commit()
                else:
                    self.schedule.rollback(instruction.transaction_id)


if __name__ == '__main__':
    # print('Optimistic Concurrency Control (Validation Based Protocol)')
    # print('=================')
    # print('Input your schedule:')
    # sequence = input()

    # sequence = 'R1(b);R2(b);R2(a);W2(b);W2(a);R1(a);C1;C2'
    # sequence = 'R1(A);R2(A);W1(A);C1;C2'
    # sequence = 'R1(A);W2(A);W2(B);W3(B);W1(A);C1;C2;C3'
    sequence = 'R2(A);R1(A);W1(A);R2(B);W2(A);W1(B);C1;C2'
    occ = OCC(sequence)
    # for q in occ.schedule.instructions.queue:
    #     print(q)
    occ._transform_schedule()
    for q in occ.schedule.instructions.queue:
        print(q)
    print('====')
    occ.run()
    print('==============')
    logs = [ins.__str__() for ins in occ.schedule.logging.queue]
    print(';'.join(logs))

    # occ.run()


