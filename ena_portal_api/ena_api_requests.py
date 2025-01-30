import logging
from typing import List, Literal, Optional, Type, TypeVar, Union
import httpx
from httpx import Auth, Response
from pydantic import BaseModel, Field, field_serializer, model_validator
from ena_portal_api.constants import ENAPortalResultType
from ena_portal_api.models.read_run import ReadRunFields, ReadRunQuery
from ena_portal_api.models.study import ENAStudyFields, ENAStudyQuery
from ena_portal_api.query.base import BaseENAQueryConditions, ENAQueryClause, ENAQueryPair


ENAQuerySetType = TypeVar("ENAQuerySetType", bound="_ENAQueryConditions")


class ENAAPIRequest(BaseModel):
    result: ENAPortalResultType
    # query: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair]
    query: Optional[Union[BaseENAQueryConditions, ENAQueryClause, ENAQueryPair]] = Field(default=None)
    # query: Optional[Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair]] = None
    # fields: Union[List[ENAStudyFields],  List[ReadRunFields]]
    fields: Union[List[Union[ENAStudyFields, ReadRunFields]]]
    limit: Optional[int] = Field(None, description="Max number of results to return")
    format: Literal["tsv", "json"] = Field("json")

    @model_validator(mode="after")
    def result_and_query_and_return_fields_align(self):
        if self.result == ENAPortalResultType.STUDY:
            assert isinstance(self.fields, List)
            assert isinstance(self.fields[0], ENAStudyFields)
            self._assert_query_conditions_are_of_type(self.query, self.result)

    def _assert_query_conditions_are_of_type(
        self,
        query_part: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair],
        result_type: ENAPortalResultType,
    ):
        if type(query_part) == ENAQuerySetType:
            if result_type == ENAPortalResultType.STUDY:
                assert isinstance(query_part, ENAStudyQuery)
            if result_type == ENAPortalResultType.READ_RUN:
                assert isinstance(query_part, ReadRunQuery)
            elif isinstance(query_part, ENAQueryPair):
                self._assert_query_conditions_are_of_type(query_part.left, result_type)
                self._assert_query_conditions_are_of_type(query_part.right, result_type)

    @field_serializer("query")
    def serialize_query(self, query: Type[BaseENAQueryConditions], _info):
        return f'"{query}"'
        # return "%22" + str(query).replace(" ", "%20") + "%22"  # e.g. '"key1=val1 AND key2=val2"', encoded

    # @field_serializer("fields")
    # def serialize_fields(self, fields: Union[List[ENAStudyFields]], _info):
    #     return ",".join(fields)

    @field_serializer("fields")
    def serialize_fields(self, fields: List[Union[ENAStudyFields, ReadRunFields]], _info):
        # return ",".join(field.value for field in fields)
        return ",".join(fields)

    @field_serializer("result")
    def serialize_result_type(self, result: ENAPortalResultType):
        return result.value

    def get(self, auth: Type[Auth] = None) -> Response:
        # url = EMG_CONFIG.ena.portal_search_api
        url = "https://www.ebi.ac.uk/ena/portal/api/search"
        params = self.model_dump()
        r = httpx.get(
            url=url,
            params=params,
            auth=auth,
        )
        logging.warning(r.request)
        return r


def create_ena_api_request(result_type, query, limit, fields, result_format="json"):
    return (
        # f"{EMG_CONFIG.ena.portal_search_api}?"
        "https://www.ebi.ac.uk/ena/portal/api/search?"
        f"result={result_type}&"
        f"query={query}&"
        f"limit={limit}&"
        f"format={result_format}&"
        f"fields={fields}"
    )
