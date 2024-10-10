from collections.abc import Sequence

import dlt
from dlt.sources import DltResource
from pyalex import Authors, Funders, Institutions, Publishers, Sources, Topics, Works, config
from pydantic import BaseModel


@dlt.source
def open_alex(email: str) -> Sequence[DltResource]:
    """
    """

    config.email = email

    @dlt.resource(name="work", write_disposition="merge", primary_key="id")
    def work(_filter:dict):
        # NOTE: filter() accepts kwargs so I can pass it a dictionary
        pager = Works().filter(_filter).paginate(per_page=200)
        for works in pager:
            yield from works

    @dlt.resource(name="author", write_disposition="merge", primary_key="id")
    def author(_filter:dict):
        pager = Authors().filter(_filter).paginate(per_page=200)
        for authors in pager:
            yield from authors

    @dlt.resource(name="institution", write_disposition="merge", primary_key="id")
    def institution(_filter:dict):
        pager = Institutions().filter(_filter).paginate(per_page=200)
        for institutions in pager:
            yield from institutions

    @dlt.resource(name="topic", write_disposition="merge", primary_key="id")
    def topic(_filter:dict):
        pager = Topics().filter(_filter).paginate(per_page=200)
        for topics in pager:
            yield from topics

    @dlt.resource(name="source", write_disposition="merge", primary_key="id")
    def source(_filter:dict):
        pager = Sources().filter(_filter).paginate(per_page=200)
        for sources in pager:
            yield from sources

    @dlt.resource(name="publisher", write_disposition="merge", primary_key="id")
    def publisher(_filter:dict):
        pager = Publishers().filter(_filter).paginate(per_page=200)
        for publishers in pager:
            yield from publishers

    @dlt.resource(name="funder", write_disposition="merge", primary_key="id")
    def funder(_filter):
        pager = Funders().filter(_filter).paginate(per_page=200)
        for funders in pager:
            yield from funders

    return work, author, institution, topic, source, publisher, funder

