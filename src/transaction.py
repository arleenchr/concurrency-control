from typing import List
from instruction import Instruction


class Transaction:
    def __init__(self, id: str, instructions:List[Instruction]) -> None:
        self.id:str = id
        self.instructions: List[Instruction] = instructions
