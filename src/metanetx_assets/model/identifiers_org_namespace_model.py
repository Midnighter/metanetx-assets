# Copyright (c) 2019, Moritz E. Beber.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Provide data validation for the Identifiers.org namespaces."""


from datetime import datetime

from pydantic import BaseModel, Field


class IdentifiersOrgNamespaceModel(BaseModel):
    """
    Define an Identifiers.org namespace response data model [1]_.

    References
    ----------
    .. [1] https://docs.identifiers.org/articles/api.html#namespaces

    """

    prefix: str
    miriam_id: str = Field(
        ..., alias="mirId", regex=r"^MIR:\d{8}$", min_length=12, max_length=12
    )
    name: str
    pattern: str
    description: str
    embedded_prefix: bool = Field(..., alias="namespaceEmbeddedInLui")
    created_on: datetime = Field(..., alias="created")
    updated_on: datetime = Field(..., alias="modified")
