import logging
from typing import Literal, TypeVar

import httpx
from httpx import Auth, Response
from pydantic import BaseModel, Field, field_serializer, model_validator

from ena_portal_api.constants import ENAPortalResultType
from ena_portal_api.models.read_run import ReadRunFields
from ena_portal_api.models.study import StudyFields
from ena_portal_api.query.base import BaseENAQueryConditions, ENAQueryClause, ENAQueryPair

ENAQuerySetType = TypeVar("ENAQuerySetType", bound="_ENAQueryConditions")

from typing import Type, Tuple, Union, List, Optional
import importlib


class ENAAPIRequest(BaseModel):
    result: ENAPortalResultType
    query: Optional[Union[BaseENAQueryConditions, ENAQueryClause, ENAQueryPair]] = Field(default=None)
    fields: Union[List[Union[StudyFields, ReadRunFields]]]
    limit: Optional[int] = Field(default=None, description="Max number of results to return")
    format: Literal["tsv", "json"] = Field(default="json")

    def _get_types_for_result(self) -> Tuple[Type, Type]:
        """
        Dynamically get the Fields and Query types based on result type.
        Convention:
        - For result type 'study' -> StudyFields and StudyQuery in models.study
        - For result type 'read_run' -> ReadRunFields and ReadRunQuery in models.read_run
        etc.
        """
        # Convert enum value (e.g., 'read_run') to module and class names
        result_type = self.result.value

        # Convert snake_case to PascalCase for class names
        class_prefix = ''.join(word.capitalize() for word in result_type.split('_'))
        fields_class_name = f'{class_prefix}Fields'
        query_class_name = f'{class_prefix}Query'

        # Convert to module path (e.g., 'read_run' -> 'models.read_run')
        module_name = f"ena_portal_api.models.{result_type}"

        try:
            # Import the appropriate module
            module = importlib.import_module(module_name)

            # Get the classes from the module
            fields_class = getattr(module, fields_class_name)
            query_class = getattr(module, query_class_name)

            return fields_class, query_class
        except (ImportError, AttributeError) as e:
            raise ValueError(
                f"Could not find required classes for result type '{result_type}'. "
                f"Expected classes '{fields_class_name}' and '{query_class_name}' "
                f"in module '{module_name}'. "
                f"Original error: {str(e)}"
            )


    @model_validator(mode="after")
    def result_and_query_and_return_fields_align(self):
        if self.query is None:
            return self

        # Get the expected field and query types for this result type
        field_type, query_type = self._get_types_for_result()

        # Validate fields
        assert isinstance(self.fields, List), "Fields must be a list"
        if self.fields:  # Only validate if fields list is not empty
            assert isinstance(self.fields[0], field_type), \
                f"Fields must be of type {field_type.__name__}, got {type(self.fields[0]).__name__}"

        # Validate query
        if isinstance(self.query, BaseENAQueryConditions):
            assert isinstance(self.query, query_type), \
                f"Query must be of type {query_type.__name__}, got {type(self.query).__name__}"
        elif isinstance(self.query, ENAQueryPair):
            self._validate_query_pair_types(self.query, query_type)

        return self


    def _validate_query_pair_types(self, query_pair: ENAQueryPair, expected_type: Type[BaseENAQueryConditions]):
        for part in [query_pair.left, query_pair.right]:
            if isinstance(part, BaseENAQueryConditions):
                assert isinstance(part, expected_type), \
                    f"Query condition must be of type {expected_type.__name__}, got {type(part).__name__}"
            elif isinstance(part, ENAQueryPair):
                self._validate_query_pair_types(part, expected_type)

    @field_serializer("query")
    def serialize_query(self, query: Type[BaseENAQueryConditions], _info):
        return f'"{query}"'
        # return "%22" + str(query).replace(" ", "%20") + "%22"  # e.g. '"key1=val1 AND key2=val2"', encoded


    # @field_serializer("fields")
    # def serialize_fields(self, fields: Union[List[ENAStudyFields]], _info):
    #     return ",".join(fields)

    @field_serializer("fields")
    def serialize_fields(self, fields: List[Union[StudyFields, ReadRunFields]], _info):
        # return ",".join(field.value for field in fields)
        return ",".join(fields)


    @field_serializer("result")
    def serialize_result_type(self, result: ENAPortalResultType):
        return result.value


    def get(self, auth: Type[Auth] = None) -> Response:
        # url = EMG_CONFIG.ena.portal_search_api
        url = "https://www.ebi.ac.uk/ena/portal/api/search"
        params = self.model_dump()
        # output the contents of params
        print('logging params')
        logging.warning(params['query'])
        if "None" in params['query']:
            del params['query']
        # logging.warning(params['query'])
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
