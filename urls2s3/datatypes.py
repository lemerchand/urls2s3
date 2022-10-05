from enum import Enum


class Status(Enum):
    PENDING = 0
    SUCCESSFUL = 1
    FAILED = 2

    def __str__(self):
        match self.name:
            case 'PENDING':
                return 'Pending'
            case 'SUCCESSFUL':
                return 'Successful'
            case 'FAILED':
                return 'Failed'
