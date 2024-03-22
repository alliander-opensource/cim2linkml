import os
from dataclasses import dataclass
from typing import Iterable

QEAProjectFile = os.PathLike | str
ObjectID = int
UMLClassName = str
UMLAttributeName = str


@dataclass
class UMLAttribute:
    id: ObjectID
    name: UMLAttributeName
    lower_bound: int
    upper_bound: int
    note: str | None
    stereotype: str | None


@dataclass
class UMLClass:
    id: ObjectID
    name: UMLClassName
    package_id: int
    attributes: dict[UMLAttributeName, UMLAttribute]
    note: str | None
    stereotype: str | None
