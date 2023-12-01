from schedule import Schedule
from collections import deque
from queue import Queue

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
                else:
                    self.schedule.rollback(instruction.transaction_id)




if __name__ == '__main__':
    # print('Optimistic Concurrency Control (Validation Based Protocol)')
    # print('=================')
    # print('Input your schedule:')
    # sequence = input()

    sequence = 'R1(b);R2(b);R2(a);W2(b);W2(a);R1(a);C1;C2'
    occ = OCC(sequence)
    # for q in occ.schedule.instructions.queue:
    #     print(q)
    # print('---')
    occ._transform_schedule()
    for q in occ.schedule.instructions.queue:
        print(q)
    print('====')
    occ.run()
    print('!!!')
    for log in occ.schedule.logging.queue:
        print(log)


    # occ.run()


