import sqlite3
from pprint import pprint
from textwrap import dedent
from itertools import groupby, islice, tee
from operator import itemgetter

from linkml.utils.schema_builder import SchemaBuilder
from linkml_runtime.utils.formatutils import underscore, uncamelcase, camelcase
from linkml_runtime.utils.schema_as_dict import schema_as_yaml_dump
from linkml_runtime.linkml_model import SchemaDefinition, ClassDefinition, SlotDefinition, EnumDefinition, PermissibleValue, Annotation


PRIMITIVES = {
    "Float": "float",
    "Integer": "integer",
    "DateTime": "date",
    "Duration": "float",
    "String": "string",
    "Time": "datetime",
    "Boolean": "boolean",
    "Decimal": "double",  # Is this correct?
    "MonthDay": "date",  # Is this correct?
    "Date": "date"
}

def _gen_curie(name, prefix):
    return f"{prefix}:{name.strip().replace(' ', '').replace('%', 'percent')}"


def generate_linkml_schema(rows):
    schema_builder = SchemaBuilder(id="http://w3id.org/cim", name="cim-package")\
        .add_defaults()\
        .add_prefix("cim", "http://iec.ch/TC57/CIM100#")

    for _, obj in groupby(rows, itemgetter("object_id")):
        obj = list(obj)
        obj_name = obj[0]["class_name"]
        schema_builder.add_class(ClassDefinition(
            name="CIMDatatype",
            abstract=True,
            annotations=Annotation(tag="data_type", value=True),
            attributes={
                "unit": SlotDefinition(name="unit", range="UnitSymbol"),
                "multiplier": SlotDefinition(name="multiplier", range="UnitMultiplier"),
                "value": SlotDefinition(name="value", range="float")}))

        match obj[0]["class_stereotype"]:
            case "enumeration":
                schema_builder.add_enum(
                    EnumDefinition(name=obj_name,
                                   enum_uri=_gen_curie(obj_name, "cim"),
                                   description=obj[0]["class_note"],
                                   permissible_values={val_name: PermissibleValue(
                                                           text=val_name,
                                                           description=row["attr_note"],
                                                           meaning=_gen_curie(f"{obj_name}.{val_name}", "cim"))
                                                       for row in obj if (val_name := row["attr_name"])}),
                    replace_if_present=True)
            case "Primitive":
                continue
            case "CIMDatatype":
                schema_builder.add_class(
                    ClassDefinition(name=obj_name,
                                    description=obj[0]["class_note"],
                                    class_uri=_gen_curie(obj_name, "cim"),
                                    is_a="CIMDatatype"))
            case _:
                schema_builder.add_class(
                    ClassDefinition(name=obj_name,
                                    description=obj[0]["class_note"],
                                    class_uri=_gen_curie(obj_name, "cim"),
                                    attributes={underscore(uncamelcase(attr_name)): SlotDefinition(
                                                    name=underscore(uncamelcase(attr_name)),
                                                    description=row["attr_note"],
                                                    range=PRIMITIVES.get(row["attr_type"], row["attr_type"]),
                                                    multivalued=True if int(row["attr_upper_bound"]) > 1 else False,
                                                    required=True if int(row["attr_lower_bound"]) > 0 else False,
                                                    slot_uri=_gen_curie(f"{obj_name}.{attr_name}", "cim"))
                                                for row in obj if (attr_name := row["attr_name"])}),
                    replace_if_present=True)
                

    return schema_builder.schema


def write_schema(schema, path):
    with open(path, "w") as f:
        f.write(schema_as_yaml_dump(schema))


def get_ea_objects(conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    return cur.execute(dedent("""
        select
            c.Object_ID as object_id,
            a.ID as attribute_id,
            c.Name as class_name,
            c.Note as class_note,
            c.Package_ID as class_package_id,
            c.Stereotype as class_stereotype,
            a.Name as attr_name,
            a.Notes as attr_note,
            a.Type as attr_type,
            a.Stereotype as attr_stereotype,
            a.LowerBound as attr_lower_bound,
            a.UpperBound as attr_upper_bound
        from t_object as c

        left join t_attribute as a
        on a.Object_ID = c.Object_ID

        where c.Object_Type == "Class"

        order by c.Object_ID, a.ID
    """))
    

if __name__ == "__main__":
    db = "data/iec61970cim17v40_iec61968cim13v13b_iec62325cim03v17b_CIM100.1.1.1.qea"
    conn = sqlite3.connect(db)
    ea_objects = get_ea_objects(conn)
    schema = generate_linkml_schema(ea_objects)
    write_schema(schema, "out.yml")
    
    
