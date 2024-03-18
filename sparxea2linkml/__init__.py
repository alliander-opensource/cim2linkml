import pprint
import sqlite3
import textwrap


if __name__ == "__main__":
    db = "data/iec61970cim17v40_iec61968cim13v13b_iec62325cim03v17b_CIM100.1.1.1.qea"
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    classes = cur.execute(textwrap.dedent("""
        select
            c.Object_ID,
            c.Name as ClassName,
            c.Note as ClassDoc,
            c.Package_ID,
            c.Stereotype as ClassStereotype,
            a.Name as AttributeName,
            a.Notes as AttributeDoc,
            a.Stereotype as AttributeStereotype,
            a.LowerBound,
            a.UpperBound
        from t_object as c

        left join t_attribute as a
        on a.Object_ID = c.Object_ID

        where c.Object_Type == "Class"

        order by c.Object_ID
    """))

    for class_ in classes:
        pprint.pprint(class_)
