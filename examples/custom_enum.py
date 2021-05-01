from donphan import Column, Enum, Table


class MyEnum(Enum):
    A = 1
    B = 2
    C = 3


class MyTable(Table):
    key: Column[str] = Column(primary_key=True)
    value: Column[MyEnum]
