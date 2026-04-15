from neomodel import (
    AsyncStructuredNode,
    AsyncRelationshipTo,
    AsyncRelationshipFrom,
    AsyncOne,
    StringProperty,
    IntegerProperty,
    ArrayProperty,
    FloatProperty,
)


SECTION_NAMES = {
    "item1": "Business",
    "item1a": "Risk Factors",
    "item7": "MD&A",
    "item7a": "Market Risk Disclosures",
}


class Form(AsyncStructuredNode):
    form_id = StringProperty(unique_index=True, required=True)
    cik = IntegerProperty(required=True)
    cusip6 = StringProperty(required=True)
    source = StringProperty()
    summary = StringProperty()
    names = ArrayProperty(StringProperty())



class Section(AsyncStructuredNode):
    section_id = StringProperty(unique_index=True, required=True)
    item = StringProperty(required=True)
    name = StringProperty(required=True)
    form_id = StringProperty(required=True)
    text_embedding = ArrayProperty(FloatProperty())



class Chunk(AsyncStructuredNode):
    chunk_id = StringProperty(unique_index=True, required=True)
    form_id = StringProperty(required=True)
    item = StringProperty(required=True)
    sequence = IntegerProperty(required=True)
    cik = IntegerProperty(required=True)
    cusip6 = StringProperty(required=True)
    text = StringProperty(required=True)
    names = ArrayProperty(StringProperty())
    text_embedding = ArrayProperty(FloatProperty())

class Company(AsyncStructuredNode):
    cik = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(required=True)
    cusip6 = StringProperty()
    address = StringProperty()


class Manager(AsyncStructuredNode):
    manager_cik = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(required=True)
    address = StringProperty()

