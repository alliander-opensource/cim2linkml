## Assumptions and Modeling Choices

* CIM primitive types are mapped onto LinkML ones and the classes are ignored
* Relations are assumed to be binary only (no n-ary relations)
* All relation types are flatly mapped onto LinkML attributes
* Enumeration classes map onto LinkML classes
* Class attributes are kept inlined in LinkML as well (no top-level slots)
* For URI generation a simple HTML escape encoding is performed where needed (spaces, '%' signs)

### Relations

* Currently only generalization relations are treated specially, all others are treated the same.
* Sub types are also ignored.

* _Generalization_ relations map onto `is_a` slots
    * Direction is assumed from start to end object, even if not explicitly modeled as such
* All other types of relations 