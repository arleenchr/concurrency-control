from enum import Enum

class InstructionType(Enum):
    READ = 'read'
    WRITE = 'write'
    COMMIT = 'commit'
    ABORT = 'abort'


class TransactionValidationBasedPhase(Enum):
    READ_AND_EXECUTION = 1
    VALIDATION = 2
    WRITE = 3
