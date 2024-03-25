import os
import sqlite3
import textwrap
import urllib.parse
from itertools import groupby
from operator import itemgetter, attrgetter
from collections.abc import Iterable, Iterator
from pprint import pprint
from typing import Literal

from linkml_runtime.utils.schema_as_dict import schema_as_yaml_dump
from linkml_runtime.utils.formatutils import uncamelcase, underscore
from linkml_runtime import linkml_model

from sparxea2linkml import ea_model

YAMLFilePath = os.PathLike | str
URI = str
CURIE = str
LinkMLTypes = Literal[
    "string",
    "integer",
    "boolean",
    "float",
    "double",
    "decimal",
    "time",
    "date",
    "datetime",
    "date_or_datetime",
    "uriorcurie",
    "uri",
    "curie",
    "ncname",
    "objectidentifier",
    "nodeidentifier",
    "jsonpointer",
    "jsonpath",
    "sparqlpath",
]


def read_uml_relations(conn: sqlite3.Connection) -> sqlite3.Cursor:
    cur = conn.cursor()

    query = textwrap.dedent(
        """
        select
            relation.Connector_Type,
            relation.Start_Object_ID,
            relation.SourceCard,
            relation.SourceRole,
            relation.SourceRoleNote,
            relation.End_Object_ID,
            relation.DestCard,
            relation.DestRole,
            relation.DestRoleNote
        from t_connector as relation
        """
    )
    uml_relation_rows = cur.execute(query)

    return uml_relation_rows


# def generate_class(uml_class: ea_model.UMLClass, kind=None) -> linkml_model.ClassDefinition:
#     class_ = linkml_model.ClassDefinition(
#         name=uml_class.name,
#         class_uri=generate_curie("cim", uml_class.name),
#     )
#     match kind:
#         case "CIMDatatype":
#             attributes = {
#                 underscore(uncamelcase(attr.name)): linkml_model.SlotDefinition(
#                     name=underscore(uncamelcase(attr.name)),
#                     range=(
#                         map_primitive_data_type(attr.type)
#                         if range_class.stereotype == "Primitive"
#                         else attr.type
#                     ),
#                     slot_uri=generate_curie("cim", f"{uml_class.name}.{attr.name}"),
#                 )
#                 for attr in uml_class.attributes.values()
#                 if attr.id is not None and (range_class := uml_classes_by_name[attr.type])
#             }

#             class_.attributes = (attributes,)
#             class_._ea_object_id = uml_class.id
#         case _:
#             ...

#     attributes = {
#         underscore(uncamelcase(attr.name)): linkml_model.SlotDefinition(
#             name=underscore(uncamelcase(attr.name)),
#             range=(
#                 map_primitive_data_type(attr.type)
#                 if range_class.stereotype == "Primitive"
#                 else attr.type
#             ),
#             slot_uri=generate_curie("cim", f"{uml_class.name}.{attr.name}"),
#         )
#         for attr in uml_class.attributes.values()
#         if attr.id is not None and (range_class := uml_classes_by_name[attr.type])
#     }

#     class_ = linkml_model.ClassDefinition(
#         name=uml_class.name,
#         class_uri=generate_curie("cim", uml_class.name),
#         attributes=attributes,
#     )
#     class_._ea_object_id = uml_class.id


def read_uml_classes(conn: sqlite3.Connection) -> sqlite3.Cursor:
    cur = conn.cursor()

    query = textwrap.dedent(
        """
        select
            class.Object_ID,
            class.Name,
            class.Package_ID,
            class.Note,
            class.Stereotype,
            attribute.ID,
            attribute.Name,
            attribute.LowerBound,
            attribute.UpperBound,
            attribute.Type,
            attribute.Notes,
            attribute.Stereotype
        from t_object as class

        left join t_attribute as attribute
        on attribute.Object_ID = class.Object_ID

        where class.Object_Type == "Class"

        order by class.Object_ID, attribute.ID
        """
    )
    uml_class_rows = cur.execute(query)

    return uml_class_rows


def parse_uml_classes(
    uml_class_rows: Iterable[sqlite3.Row],
) -> Iterator[tuple[ea_model.ObjectID, ea_model.UMLClass]]:
    for object_id, rows in groupby(uml_class_rows, itemgetter(0)):
        rows = list(rows)

        if not rows:
            return

        uml_class = ea_model.UMLClass(
            id=object_id,
            name=rows[0][1],
            note=rows[0][2],
            package_id=rows[0][3],
            attributes={
                row[5]: ea_model.UMLAttribute(
                    id=row[5],
                    name=row[6],
                    lower_bound=int(row[7]) if row[7] is not None else 0,
                    upper_bound=int(row[8]) if row[8] is not None else 1,
                    type=row[9],
                    note=row[10],
                    stereotype=row[11],
                )
                for row in rows
            },
            stereotype=rows[0][4],
        )

        yield object_id, uml_class


def parse_cardinality_value(val: tuple[str, str] | None) -> ea_model.UMLCardinality:
    if val is None:
        return None

    lower, _, upper = val.partition("..")

    if upper == "":
        upper = lower

    return tuple(map(lambda v: ea_model.MANY if v in ["n", "*"] else int(v), (lower, upper)))


def map_connector_type(val: str) -> ea_model.UMLRelationType:
    match val:
        case "Abstraction":
            return ea_model.UMLRelationType.ABSTRACTION
        case "Aggregation":
            return ea_model.UMLRelationType.AGGREGATION
        case "Assembly":
            return ea_model.UMLRelationType.ASSEMBLY
        case "Association":
            return ea_model.UMLRelationType.ASSOCIATION
        case "Collaboration":
            return ea_model.UMLRelationType.COLLABORATION
        case "Communicationpath":
            return ea_model.UMLRelationType.COMMUNICATIONPATH
        case "Connector":
            return ea_model.UMLRelationType.CONNECTOR
        case "Controlflow":
            return ea_model.UMLRelationType.CONTROLFLOW
        case "Delegate":
            return ea_model.UMLRelationType.DELEGATE
        case "Dependency":
            return ea_model.UMLRelationType.DEPENDENCY
        case "Deployment":
            return ea_model.UMLRelationType.DEPLOYMENT
        case "Erlink":
            return ea_model.UMLRelationType.ERLINK
        case "Extension":
            return ea_model.UMLRelationType.EXTENSION
        case "Generalization":
            return ea_model.UMLRelationType.GENERALIZATION
        case "Informationflow":
            return ea_model.UMLRelationType.INFORMATIONFLOW
        case "Instantiation":
            return ea_model.UMLRelationType.INSTANTIATION
        case "Interruptflow":
            return ea_model.UMLRelationType.INTERRUPTFLOW
        case "Manifest":
            return ea_model.UMLRelationType.MANIFEST
        case "Nesting":
            return ea_model.UMLRelationType.NESTING
        case "Notelink":
            return ea_model.UMLRelationType.NOTELINK
        case "Objectflow":
            return ea_model.UMLRelationType.OBJECTFLOW
        case "Package":
            return ea_model.UMLRelationType.PACKAGE
        case "Protocolconformance":
            return ea_model.UMLRelationType.PROTOCOLCONFORMANCE
        case "Protocoltransition":
            return ea_model.UMLRelationType.PROTOCOLTRANSITION
        case "Realisation":
            return ea_model.UMLRelationType.REALISATION
        case "Sequence":
            return ea_model.UMLRelationType.SEQUENCE
        case "Stateflow":
            return ea_model.UMLRelationType.STATEFLOW
        case "Substitution":
            return ea_model.UMLRelationType.SUBSTITUTION
        case "Usage":
            return ea_model.UMLRelationType.USAGE
        case "Usecase":
            return ea_model.UMLRelationType.USECASE


def parse_uml_relations(
    uml_relation_rows: Iterable[sqlite3.Row],
) -> Iterator[tuple[ea_model.ObjectID, ea_model.UMLRelation]]:
    for row in uml_relation_rows:
        start_object_id = row[1]
        uml_relation = ea_model.UMLRelation(
            connector_type=map_connector_type(row[0]),
            start_object_id=start_object_id,
            source_card=parse_cardinality_value(row[2]),
            source_role=row[3],
            source_role_note=row[4],
            end_object_id=row[5],
            dest_card=parse_cardinality_value(row[6]),
            dest_role=row[7],
            dest_role_note=row[8],
        )

        yield start_object_id, uml_relation


def generate_curie(prefix: str, local_name: str) -> CURIE:
    return f"{prefix}:{urllib.parse.quote(local_name)}"


def map_primitive_data_type(val: str) -> LinkMLTypes:
    match val:
        case "Float":
            return "float"
        case "Integer":
            return "integer"
        case "DateTime":
            return "date"
        case "String":
            return "string"
        case "Boolean":
            return "boolean"
        case "Decimal":
            return "double"  # Is this right?
        case "MonthDay":
            return "date"  # Is this right?
        case "Date":
            return "date"
        case "Time":
            return "time"
        case "Duration":
            return "int"
        case _:
            raise TypeError(f"Data type `{val}` is not a CIM Primitive.")


def build_schema(
    uml_classes: Iterator[tuple[ea_model.ObjectID, ea_model.UMLClass]],
    uml_relations: Iterator[tuple[ea_model.ObjectID, ea_model.UMLRelation]],
) -> linkml_model.SchemaDefinition:
    schema = linkml_model.SchemaDefinition(
        id="http://w3id.org/cim",
        name="cim",
        title="CIM",
        prefixes={"cim": "https://cim.ucaiug.io/ns#", "linkml": "https://w3id.org/linkml/"},
        default_prefix="cim",
    )
    uml_classes: dict[ea_model.ObjectID, ea_model.UMLClass] = {
        object_id: uml_class for object_id, uml_class in uml_classes
    }
    uml_classes_by_name = {
        name: next(class_) for name, class_ in groupby(uml_classes.values(), attrgetter("name"))
    }
    uml_relations: list[ea_model.UMLRelation] = list(r for _, r in uml_relations)

    for uml_class in uml_classes.values():
        match uml_class.stereotype:
            case "Primitive":
                continue
            # case "CIMDatatype":
            #     ...  # TODO
            case "enumeration":
                enum_class = linkml_model.EnumDefinition(
                    name=uml_class.name,
                    enum_uri=generate_curie("cim", uml_class.name),
                    permissible_values={
                        attr.name: linkml_model.PermissibleValue(
                            text=attr.name,
                            meaning=generate_curie("cim", f"{uml_class.name}.{attr.name}"),
                        )
                        for attr in uml_class.attributes.values()
                        if attr.id is not None
                    },
                )
                enum_class._ea_object_id = uml_class.id
                schema.enums[uml_class.name] = enum_class
            case None | _:
                attributes = {
                    underscore(uncamelcase(attr.name)): linkml_model.SlotDefinition(
                        name=underscore(uncamelcase(attr.name)),
                        range=(
                            map_primitive_data_type(attr.type)
                            if range_class.stereotype == "Primitive"
                            else attr.type
                        ),
                        required=True if attr.lower_bound > 1 else False,
                        multivalued=True if attr.upper_bound > 1 else False,
                        slot_uri=generate_curie("cim", f"{uml_class.name}.{attr.name}"),
                    )
                    for attr in uml_class.attributes.values()
                    if attr.id is not None and (range_class := uml_classes_by_name[attr.type])
                }

                class_ = linkml_model.ClassDefinition(
                    name=uml_class.name,
                    class_uri=generate_curie("cim", uml_class.name),
                    attributes=attributes,
                )
                class_._ea_object_id = uml_class.id
                schema.classes[uml_class.name] = class_

    classes_by_ea_obj_id = {
        k: next(v) for k, v in groupby(schema.classes.values(), attrgetter("_ea_object_id"))
    }

    for uml_relation in uml_relations:
        try:
            # Assumptions: only non-enum classes have relations.
            source_class, dest_class = itemgetter(
                uml_relation.start_object_id, uml_relation.end_object_id
            )(classes_by_ea_obj_id)
        except KeyError:
            # If key is not found, we assume it's a object type different than class (like package) so we ignore it for convenience's sake.
            continue

        match uml_relation.connector_type:
            case ea_model.UMLRelationType.GENERALIZATION:
                super_class_name = dest_class.name
                source_class.is_a = super_class_name
                continue
            case _:
                # TODO: Add annotation of relation type.
                # Source end
                role_name = uml_relation.dest_role if uml_relation.dest_role else dest_class.name
                source_attr = linkml_model.SlotDefinition(
                    name=underscore(uncamelcase(role_name)),
                    range=dest_class.name,
                    required=True if uml_relation.source_card[0] > 1 else False,
                    multivalued=True if uml_relation.source_card[1] > 1 else False,
                    slot_uri=generate_curie("cim", f"{source_class.name}.{role_name}"),
                )
                source_class.attributes[source_attr.name] = source_attr

                # Destination end
                role_name = (
                    uml_relation.source_role if uml_relation.source_role else source_class.name
                )
                dest_attr = linkml_model.SlotDefinition(
                    name=underscore(uncamelcase(role_name)),
                    range=source_class.name,
                    required=True if uml_relation.source_card[0] > 1 else False,
                    multivalued=True if uml_relation.source_card[1] > 1 else False,
                    slot_uri=generate_curie("cim", f"{dest_class.name}.{role_name}"),
                )
                dest_class.attributes[dest_attr.name] = dest_attr

    return schema


def write_schema(schema: linkml_model.SchemaDefinition, output: YAMLFilePath):
    with open(output, "w") as f:
        f.write(schema_as_yaml_dump(schema))


def generate_schema(cim_db: ea_model.QEAProjectFile) -> None:
    conn = sqlite3.connect(cim_db)
    uml_class_rows = read_uml_classes(conn)
    uml_classes = parse_uml_classes(uml_class_rows)
    uml_relation_rows = read_uml_relations(conn)
    uml_relations = parse_uml_relations(uml_relation_rows)
    schema = build_schema(uml_classes, uml_relations)
    write_schema(schema, "out.yml")


if __name__ == "__main__":
    generate_schema(
        cim_db="data/iec61970cim17v40_iec61968cim13v13b_iec62325cim03v17b_CIM100.1.1.1.qea"
    )
