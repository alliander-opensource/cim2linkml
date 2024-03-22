import os
import sqlite3
import textwrap
from itertools import groupby
from operator import itemgetter
from collections.abc import Iterable, Iterator
from pprint import pprint

from linkml_runtime.utils.schema_as_dict import schema_as_yaml_dump
from linkml_runtime.utils.formatutils import uncamelcase, underscore
from linkml_runtime import linkml_model

from sparxea2linkml import ea_model


YAMLFilePath = os.PathLike | str


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
                    note=row[7],
                    lower_bound=row[8],
                    upper_bound=row[9],
                    stereotype=row[10],
                )
                for row in rows
            },
            stereotype=rows[0][4],
        )

        yield object_id, uml_class


def generate_uri(name: str) -> str:
    return f"https://cim.org/{name}/"


def build_schema(
    uml_classes: Iterator[tuple[ea_model.ObjectID, ea_model.UMLClass]]
) -> linkml_model.SchemaDefinition:
    schema = linkml_model.SchemaDefinition(
        id="http://w3id.org/cim",
        name="cim",
        title="CIM",
        prefixes={"cim": "https://cim.ucaiug.io/ns#", "linkml": "https://w3id.org/linkml/"},
        default_prefix="cim",
    )

    for _, uml_class in uml_classes:
        pprint(uml_class)
        # obj = list(obj)
        # obj_name = obj[0]["class_name"]
        match uml_class.stereotype:
            case "enumeration":
                schema.enums[uml_class.name] = linkml_model.EnumDefinition(
                    name=uml_class.name,
                    enum_uri=generate_uri(uml_class.name),
                    permissible_values={
                        attr.name: linkml_model.PermissibleValue(
                            text=attr.name, meaning=generate_uri(attr.name)
                        )
                        for attr in uml_class.attributes.values()
                        if attr.id is not None  # Skip if no attributes.
                    },
                )
            case _:
                schema.classes[uml_class.name] = linkml_model.ClassDefinition(
                    name=uml_class.name,
                    class_uri=generate_uri(uml_class.name),
                    attributes={
                        underscore(uncamelcase(attr.name)): linkml_model.SlotDefinition(
                            name=underscore(uncamelcase(attr.name)),
                            slot_uri=generate_uri(attr.name),
                        )
                        for attr in uml_class.attributes.values()
                        if attr.id is not None  # Skip if no attributes.
                    },
                )

    return schema


def write_schema(schema: linkml_model.SchemaDefinition, output: YAMLFilePath):
    with open(output, "w") as f:
        f.write(schema_as_yaml_dump(schema))


def generate_schema(cim_db: ea_model.QEAProjectFile) -> None:
    conn = sqlite3.connect(cim_db)
    uml_class_rows = read_uml_classes(conn)
    uml_classes = parse_uml_classes(uml_class_rows)
    schema = build_schema(uml_classes)
    # write_schema(schema, "out.yml")


if __name__ == "__main__":
    generate_schema(
        cim_db="data/iec61970cim17v40_iec61968cim13v13b_iec62325cim03v17b_CIM100.1.1.1.qea"
    )
