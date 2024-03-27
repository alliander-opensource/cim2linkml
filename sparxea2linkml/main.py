import os
import sys
import sqlite3
import textwrap
import urllib.parse
from itertools import groupby
from operator import itemgetter
from pprint import pprint
from typing import Literal

from linkml_runtime.utils.schema_as_dict import schema_as_yaml_dump
from linkml_runtime.utils.formatutils import uncamelcase, underscore
from linkml_runtime import linkml_model

YAMLFilePath = os.PathLike | str
QEAProjectFile = os.PathLike | str
MANY = sys.maxsize
CURIE = str

UMLCardinalityValue = int
UMLCardinality = tuple[UMLCardinalityValue, UMLCardinalityValue]

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


def read_uml_classes(conn: sqlite3.Connection) -> sqlite3.Cursor:
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    query = textwrap.dedent(
        """
        SELECT
            Class.Object_ID AS ClassID,
            Class.Name AS ClassName,
            Class.Package_ID AS ClassPackageID,
            Class.Stereotype AS ClassStereotype,
            Class.Note AS ClassDescription,
            Attribute.AttrID AS AttrID,
            Attribute.RelID AS RelID,
            Attribute.Name AS AttrName,
            Attribute.Cardinality AS AttrCardinality,
            Attribute.Range AS AttrRange,
            Attribute.Description AS AttrDescription,
            Attribute.RelationType AS AttrRelationType,
            Attribute.Stereotype AS AttrStereotype,
            Attribute.RangeStereotype AS AttrRangeStereotype
        FROM t_object AS Class

        LEFT JOIN (
            SELECT
                Attr.ID AS AttrID,
                NULL AS RelID,
                Attr.Object_ID AS Object_ID,
                Attr.Name AS Name,
                Attr.LowerBound || ".." || Attr.UpperBound AS Cardinality,
                Attr.Type AS Range,
                Attr.Notes AS Description,
                NULL AS RelationType,
                Attr.Stereotype AS Stereotype,
                (SELECT C_.Stereotype FROM t_object AS C_ WHERE Attr.Type = C_.Name) AS RangeStereotype
            FROM t_attribute AS Attr

            UNION

            SELECT
                NULL AS AttrID,
                RelationFrom.Connector_ID AS RelID,
                RelationFrom.Start_Object_ID AS Object_ID,
                COALESCE(RelationFrom.DestRole, (SELECT C_.Name FROM t_object AS C_ WHERE RelationFrom.End_Object_ID = C_.Object_ID)) AS Name,
                RelationFrom.DestCard AS Cardinality,
                (SELECT C_.Name FROM t_object AS C_ WHERE RelationFrom.End_Object_ID = C_.Object_ID) AS Range,
                RelationFrom.Notes AS Description,
                RelationFrom.Connector_Type AS RelationType,
                RelationFrom.Stereotype AS Stereotype,
                (SELECT C_.Stereotype FROM t_object AS C_ WHERE RelationFrom.End_Object_ID = C_.Object_ID) AS RangeStereotype
            FROM t_connector AS RelationFrom

            UNION
            
            SELECT
                NULL AS AttrID,
                RelationTo.Connector_ID AS RelID,
                RelationTo.End_Object_ID AS Object_ID,
                COALESCE(RelationTo.SourceRole, (SELECT C_.Name FROM t_object AS C_ WHERE RelationTo.Start_Object_ID = C_.Object_ID)) AS Name,
                RelationTo.SourceCard AS Cardinality,
                (SELECT C_.Name FROM t_object AS C_ WHERE RelationTo.Start_Object_ID = C_.Object_ID) AS Range,
                RelationTo.Notes AS Description,
                RelationTo.Connector_Type AS RelationType,
                RelationTo.Stereotype AS Stereotype,
                (SELECT C_.Stereotype FROM t_object AS C_ WHERE RelationTo.End_Object_ID = C_.Object_ID) AS RangeStereotype
            FROM t_connector AS RelationTo
            WHERE RelationTo.Connector_Type != "Generalization"

        ) AS Attribute

        ON Class.Object_ID = Attribute.Object_ID
        WHERE Class.Object_Type = "Class"
        -- AND Class.Object_ID = 84
        ORDER BY Class.Object_ID, AttrID, RelID
        """
    )
    uml_class_rows = cur.execute(query)

    return uml_class_rows


def read_packages(conn: sqlite3.Connection) -> sqlite3.Cursor:
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    query = textwrap.dedent(
        """
        SELECT
            Package.Package_ID,
            Package.Name,
            Package.Parent_ID,
            Package.Notes
        FROM t_package AS Package
        """
    )
    packages = cur.execute(query)

    return packages


def parse_cardinality_value(val: tuple[str, str] | None) -> UMLCardinality:
    if val is None:
        return (0, 1)

    lower, _, upper = val.partition("..")

    if upper == "":
        upper = lower

    return tuple(map(lambda v: MANY if v in ["n", "*"] else int(v), (lower, upper)))


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


def build_schema(uml_classes: sqlite3.Cursor, package=None, pkg_path_parts=None) -> linkml_model.SchemaDefinition:
    if package:
        schema = linkml_model.SchemaDefinition(
            id=f"https://cim.ucaiug.io/ns/{'/'.join(pkg_path_parts)}",
            name=package["Name"],
            title=package["Name"],
            prefixes={"cim": "https://cim.ucaiug.io/ns#", "linkml": "https://w3id.org/linkml/"},
            default_prefix="cim",
        )
    else:
        schema = linkml_model.SchemaDefinition(
            id="https://cim.ucaiug.io/ns#CIM",  # TODO: ?
            name="cim",
            title="CIM",
            prefixes={"cim": "https://cim.ucaiug.io/ns#", "linkml": "https://w3id.org/linkml/"},
            default_prefix="cim",
        )

    for class_id, class_rows in groupby(uml_classes, itemgetter("ClassID")):
        class_rows = list(class_rows)
        match class_rows[0]["ClassStereotype"]:
            case "Primitive":
                continue
            # case "CIMDatatype":
            #     ...  # TODO
            case "enumeration":
                enum_name = class_rows[0]["ClassName"]
                enum_class = linkml_model.EnumDefinition(
                    name=enum_name,
                    enum_uri=generate_curie("cim", enum_name),
                    description=class_rows[0]["ClassDescription"],
                    permissible_values={
                        attr["AttrName"]: linkml_model.PermissibleValue(
                            text=attr["AttrName"],
                            meaning=generate_curie("cim", f"{enum_name}.{attr['AttrName']}"),
                        )
                        for attr in class_rows
                        if not (attr["AttrID"] is None and attr["RelID"] is None)
                    },
                )
                schema.enums[class_rows[0]["ClassName"]] = enum_class
            case None | _:
                class_name = class_rows[0]["ClassName"]
                attributes = {
                    underscore(uncamelcase(attr["AttrName"])): linkml_model.SlotDefinition(
                        name=underscore(uncamelcase(attr["AttrName"])),
                        range=(
                            map_primitive_data_type(attr["AttrRange"])
                            if attr["AttrRangeStereotype"] == "Primitive"
                            else attr["AttrRange"]
                        ),
                        description=attr["AttrDescription"],
                        required=(
                            True
                            if parse_cardinality_value(attr["AttrCardinality"])[0] > 1
                            else False
                        ),
                        multivalued=(
                            True
                            if parse_cardinality_value(attr["AttrCardinality"])[1] > 1
                            else False
                        ),
                        slot_uri=generate_curie("cim", f"{class_name}.{attr['AttrName']}"),
                    )
                    for attr in class_rows
                    if not (attr["AttrID"] is None and attr["RelID"] is None)
                    if attr["AttrName"] is not None
                    if attr["AttrRelationType"] != "Generalization"
                }

                try:  # TODO: Tidy up.
                    super_class_name = next(
                        attr["AttrRange"]
                        for attr in class_rows
                        if attr["AttrRelationType"] == "Generalization"
                    )
                except StopIteration:
                    super_class_name = None

                class_ = linkml_model.ClassDefinition(
                    name=class_name,
                    is_a=super_class_name,
                    class_uri=generate_curie("cim", class_name),
                    attributes=attributes,
                    description=class_rows[0]["ClassDescription"],
                )
                schema.classes[class_name] = class_

    return schema


def build_package_path(start_pkg_id, packages, package_path=None):
    if package_path is None:
        package_path = []

    package = packages[start_pkg_id]
    parent_id = package["Parent_ID"]

    if parent_id in (0, None):
        return package_path

    parent = packages[parent_id]
    return build_package_path(parent["Package_ID"], packages, package_path + [package['Name']])
    


def write_schema(schema: linkml_model.SchemaDefinition, output: YAMLFilePath):
    with open(output, "w") as f:
        f.write(schema_as_yaml_dump(schema))


def generate_schema(cim_db: QEAProjectFile, schema_per_package=False) -> None:
    conn = sqlite3.connect(cim_db)
    uml_class_rows = list(read_uml_classes(conn))
    packages_by_id = {pkg_id: next(pkg) for pkg_id, pkg in groupby(read_packages(conn), itemgetter("Package_ID"))}

    if schema_per_package:
        for package_id, package in packages_by_id.items():
            uml_class_rows_in_pkg = [c for c in uml_class_rows if c["ClassPackageID"] == package_id]

            if len(uml_class_rows_in_pkg) == 0:
                continue

            pkg_path_parts = build_package_path(package_id, packages_by_id)[::-1]

            if not pkg_path_parts:
                continue

            pkg_dirpath = os.path.join("out", os.sep.join(pkg_path_parts[:-1]))
            pkg_filename = pkg_path_parts[-1] + ".yml"
            os.makedirs(pkg_dirpath, exist_ok=True)

            schema = build_schema(uml_class_rows_in_pkg, package, pkg_path_parts)
            write_schema(schema, os.path.join(pkg_dirpath, pkg_filename))
    else:
        schema = build_schema(uml_class_rows)
        write_schema(schema, "cim.yml")


if __name__ == "__main__":
    generate_schema(
        cim_db="data/iec61970cim17v40_iec61968cim13v13b_iec62325cim03v17b_CIM100.1.1.1.qea",
        schema_per_package=True
    )
