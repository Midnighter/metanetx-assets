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


from typing import List, Optional

from pydantic import BaseModel, Field

from .identifiers_org_namespace_model import IdentifiersOrgNamespaceModel


class IdentifiersOrgNamespaceListModel(BaseModel):
    """Define the nested list of Identifiers.org namespaces."""

    namespaces: List[IdentifiersOrgNamespaceModel]


class IdentifiersOrgRegistryModel(BaseModel):
    """
    Define an Identifiers.org registry response data model [1]_.

    References
    ----------
    .. [1] https://docs.identifiers.org/articles/api.html#getdataset

    """

    payload: IdentifiersOrgNamespaceListModel
    api_version: Optional[str] = Field(None, alias="apiVersion")
    error_message: Optional[str] = Field(None, alias="errorMessage")
