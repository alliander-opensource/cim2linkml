import math
import os
import sys
from dataclasses import dataclass
from enum import Enum, auto
from typing import NewType

AttributeID = int
ObjectID = int
UMLClassName = str
UMLAttributeName = str

UMLCardinalityValue = int
UMLCardinality = tuple[UMLCardinalityValue, UMLCardinalityValue]


class UMLRelationType(Enum):
    ABSTRACTION = auto()
    AGGREGATION = auto()
    ASSEMBLY = auto()
    ASSOCIATION = auto()
    COLLABORATION = auto()
    COMMUNICATIONPATH = auto()
    CONNECTOR = auto()
    CONTROLFLOW = auto()
    DELEGATE = auto()
    DEPENDENCY = auto()
    DEPLOYMENT = auto()
    ERLINK = auto()
    EXTENSION = auto()
    GENERALIZATION = auto()
    INFORMATIONFLOW = auto()
    INSTANTIATION = auto()
    INTERRUPTFLOW = auto()
    MANIFEST = auto()
    NESTING = auto()
    NOTELINK = auto()
    OBJECTFLOW = auto()
    PACKAGE = auto()
    PROTOCOLCONFORMANCE = auto()
    PROTOCOLTRANSITION = auto()
    REALISATION = auto()
    SEQUENCE = auto()
    STATEFLOW = auto()
    SUBSTITUTION = auto()
    USAGE = auto()
    USECASE = auto()


@dataclass
class UMLAttribute:
    id: ObjectID
    name: UMLAttributeName
    lower_bound: int
    upper_bound: int
    type: str
    note: str | None
    stereotype: str | None


@dataclass
class UMLRelation:
    connector_type: UMLRelationType
    start_object_id: ObjectID
    source_card: UMLCardinality | None
    source_role: str | None
    source_role_note: str | None
    end_object_id: ObjectID
    dest_card: UMLCardinality | None
    dest_role: str | None
    dest_role_note: str | None


@dataclass
class UMLClass:
    id: ObjectID
    name: UMLClassName
    package_id: int
    attributes: dict[UMLAttributeName, UMLAttribute]
    note: str | None
    stereotype: str | None


@dataclass
class UMLClass2:
    id: ObjectID
    name: UMLClassName
    package_id: int
    stereotype: str | None
    note: str | None
    attributes: dict[UMLAttributeName, UMLAttribute]


@dataclass
class UMLAttribute2:
    id: AttributeID
    name: UMLAttributeName
    lower_bound: int
    upper_bound: int
    type: str
    note: str | None
    stereotype: str | None
