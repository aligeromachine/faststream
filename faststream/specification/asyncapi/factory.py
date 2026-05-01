from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union
import warnings

from faststream.specification.base import Specification, SpecificationFactory

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream.asgi.handlers import HttpHandler
    from faststream.specification.schema import Contact, ExternalDocs, License, Tag


class AsyncAPI(SpecificationFactory):
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        title: str = "FastStream",
        version: str = "0.1.0",
        description: str | None = None,
        terms_of_service: Optional["AnyHttpUrl"] = None,
        license: Union["License", "dict[str, Any]"] | None = None,
        contact: Union["Contact", "dict[str, Any]"] | None = None,
        tags: Sequence[Union["Tag", "dict[str, Any]"]] = (),
        external_docs: Union["ExternalDocs", "dict[str, Any]"] | None = None,
        identifier: str | None = None,
        schema_version: Literal["3.0.0", "2.6.0"] | str = "3.0.0",
    ) -> None:
        self.title = title
        self.version = version
        self.description = description
        self.terms_of_service = terms_of_service
        self.license = license
        self.contact = contact
        self.tags = tags
        self.external_docs = external_docs
        self.identifier = identifier
        self.schema_version = schema_version

        self.brokers: list[BrokerUsecase[Any, Any]] = []
        if broker:
            self.add_broker(broker)

        self.http_handlers: list[tuple[str, HttpHandler]] = []

    def add_broker(
        self,
        broker: "BrokerUsecase[Any, Any]",
        /,
    ) -> "SpecificationFactory":
        if broker not in self.brokers:
            self.brokers.append(broker)
        return self

    def add_http_route(
        self,
        path: str,
        handler: "HttpHandler",
    ) -> "SpecificationFactory":
        self.http_handlers.append((path, handler))
        return self

    def to_specification(self) -> Specification:
        if self.schema_version.startswith("3."):
            from .v3_0_0 import get_app_schema as schema_3_0
            from .v3_0_0.schema.schema import ApplicationSchema
            from .v3_0_0.schema.servers import Server
            from .v3_0_0.schema.channels import Channel
            from .v3_0_0.schema.operations import Operation
            from .v3_0_0.schema.message import Message

            list_of_schema: list[ApplicationSchema] = [schema_3_0(
                it,
                title=self.title,
                app_version=self.version,
                schema_version=self.schema_version,
                description=self.description,
                terms_of_service=self.terms_of_service,
                contact=self.contact,
                license=self.license,
                identifier=self.identifier,
                tags=self.tags,
                external_docs=self.external_docs,
                http_handlers=self.http_handlers,
            ) for  it in self.brokers]

            list_of_servers = [it.servers for it in list_of_schema]
            list_of_channels = [it.channels for it in list_of_schema]
            list_of_operations = [it.operations for it in list_of_schema]

            servers: dict[str, Server] = dict()
            for it in list_of_servers:
                if not it:
                    continue
                for key, value in it.items():
                    if key in servers:
                        warnings.warn(
                            f"Overwrite broker server for an application, server have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    servers[key] = value
            channels: dict[str, Channel] = dict()
            for itchannel in list_of_channels:
                for key, value in itchannel.items():
                    if key in channels:
                        warnings.warn(
                            f"Overwrite channel handler, channels have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    channels[key] = value
            operations: dict[str, Operation] = dict()
            for it in list_of_operations:
                for key, value in it.items():
                    if key in operations:
                        warnings.warn(
                            f"Overwrite Operation handler, operation have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    operations[key] = value

            list_of_schema[0].servers = servers
            list_of_schema[0].channels = channels
            list_of_schema[0].operations = operations

            list_of_components = [it.components for it in list_of_schema]
            list_of_messages = [it.messages for it in list_of_components if it.messages]
            list_of_schemas = [it.schemas for it in list_of_components if it.schemas]
            list_of_securitySchemes = [it.securitySchemes for it in list_of_components if it.securitySchemes]

            messages: dict[str, Message] = dict()
            for it in list_of_messages:
                if not it:
                    continue
                for key, value in it.items():
                    if key in messages:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    messages[key] = value
            schemas: dict[str, dict[str, Any]] = dict()
            for it in list_of_schemas:
                if not it:
                    continue
                for key, value in it.items():
                    if key in schemas:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    schemas[key] = value
            securitySchemes: dict[str, dict[str, Any]] = dict()
            for it in list_of_securitySchemes:
                if not it:
                    continue
                for key, value in it.items():
                    if key in securitySchemes:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    securitySchemes[key] = value
            
            if messages:
                list_of_schema[0].components.messages = messages
            if schemas:
                list_of_schema[0].components.schemas = schemas
            if securitySchemes:
                list_of_schema[0].components.securitySchemes = securitySchemes
            return list_of_schema[0]

        if self.schema_version.startswith("2.6."):
            from .v2_6_0 import get_app_schema as schema_2_6
            from .v2_6_0.schema.schema import ApplicationSchema
            from .v2_6_0.schema.servers import Server
            from .v2_6_0.schema.channels import Channel
            from .v2_6_0.schema.operations import Operation
            from .v2_6_0.schema.message import Message

            list_of_schema = [schema_2_6(
                it,
                title=self.title,
                app_version=self.version,
                schema_version=self.schema_version,
                description=self.description,
                terms_of_service=self.terms_of_service,
                contact=self.contact,
                license=self.license,
                identifier=self.identifier,
                tags=self.tags,
                external_docs=self.external_docs,
                http_handlers=self.http_handlers,
            ) for it in self.brokers]

            list_of_servers = [it.servers for it in list_of_schema]
            list_of_channels = [it.channels for it in list_of_schema]

            servers: dict[str, Server] = dict()
            for it in list_of_servers:
                if not it:
                    continue
                for key, value in it.items():
                    if key in servers:
                        warnings.warn(
                            f"Overwrite broker server for an application, server have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    servers[key] = value
            channels: dict[str, Channel] = dict()
            for itchannel in list_of_channels:
                for key, value in itchannel.items():
                    if key in channels:
                        warnings.warn(
                            f"Overwrite channel handler, channels have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    channels[key] = value

            list_of_schema[0].servers = servers
            list_of_schema[0].channels = channels

            list_of_components = [it.components for it in list_of_schema]
            list_of_messages = [it.messages for it in list_of_components if it.messages]
            list_of_schemas = [it.schemas for it in list_of_components if it.schemas]
            list_of_securitySchemes = [it.securitySchemes for it in list_of_components if it.securitySchemes]

            messages: dict[str, Message] = dict()
            for it in list_of_messages:
                if not it:
                    continue
                for key, value in it.items():
                    if key in messages:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    messages[key] = value
            schemas: dict[str, dict[str, Any]] = dict()
            for it in list_of_schemas:
                if not it:
                    continue
                for key, value in it.items():
                    if key in schemas:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    schemas[key] = value
            securitySchemes: dict[str, dict[str, Any]] = dict()
            for it in list_of_securitySchemes:
                if not it:
                    continue
                for key, value in it.items():
                    if key in securitySchemes:
                        warnings.warn(
                            f"Overwrite broker Message for an application, Message have the same names: `{key}`",
                            RuntimeWarning,
                            stacklevel=1,
                        )
                    securitySchemes[key] = value
            
            if messages:
                list_of_schema[0].components.messages = messages
            if schemas:
                list_of_schema[0].components.schemas = schemas
            if securitySchemes:
                list_of_schema[0].components.securitySchemes = securitySchemes
            return list_of_schema[0]

        msg = f"Unsupported schema version: {self.schema_version}"
        raise NotImplementedError(msg)
