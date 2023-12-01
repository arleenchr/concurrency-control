from typing import List
from instruction import Instruction


class Transaction:
    def __init__(self, id: int, instructions:List[Instruction]) -> None:
        self.id:int = id
        self.instructions: List[Instruction] = instructions
