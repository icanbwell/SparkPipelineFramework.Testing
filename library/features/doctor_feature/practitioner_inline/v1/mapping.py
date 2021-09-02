from typing import Any, Dict

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper_fhir.complex_types.address import Address
from spark_auto_mapper_fhir.complex_types.codeable_concept import CodeableConcept
from spark_auto_mapper_fhir.complex_types.coding import Coding
from spark_auto_mapper_fhir.complex_types.human_name import HumanName
from spark_auto_mapper_fhir.complex_types.identifier import Identifier
from spark_auto_mapper_fhir.fhir_types.id import FhirId
from spark_auto_mapper_fhir.fhir_types.list import FhirList
from spark_auto_mapper_fhir.resources.practitioner import Practitioner
from spark_auto_mapper_fhir.value_sets.address_type import AddressTypeCode
from spark_auto_mapper_fhir.value_sets.address_use import AddressUseCodeValues
from spark_auto_mapper_fhir.value_sets.administrative_gender import (
    AdministrativeGenderCode,
)
from spark_auto_mapper_fhir.value_sets.identifier_type_codes import (
    IdentifierTypeCodesCodeValues,
)
from spark_auto_mapper_fhir.value_sets.identifier_use import (
    IdentifierUseCode,
    IdentifierUseCodeValues,
)
from spark_auto_mapper_fhir.value_sets.name_use import NameUseCodeValues


def mapping(parameters: Dict[str, Any]) -> AutoMapper:
    mapper = AutoMapper(
        view=parameters["view"],
        source_view="bwellProviderFeed_08122020",  # file name
        keys=["gecb_provider_number"],
    ).complex(
        Practitioner(
            id_=FhirId(A.column("b.gecb_provider_number")),
            identifier=FhirList(
                [
                    Identifier(
                        use=IdentifierUseCodeValues.Usual,
                        value=A.column("b.gecb_provider_number"),
                        type_=CodeableConcept(
                            coding=FhirList(
                                [
                                    Coding(
                                        system=IdentifierUseCode.codeset,
                                        code=IdentifierTypeCodesCodeValues.ProviderNumber,
                                    )
                                ]
                            )
                        ),
                        system="medstarhealth.org",
                    ),
                    Identifier(
                        use=IdentifierUseCodeValues.Official,
                        value=A.column("provider_npi"),
                        type_=CodeableConcept(
                            coding=FhirList(
                                [
                                    Coding(
                                        system=IdentifierUseCode.codeset,
                                        code=IdentifierTypeCodesCodeValues.NationalProviderIdentifier,
                                    )
                                ]
                            )
                        ),
                        system="http://hl7.org/fhir/sid/us-npi",
                    ),
                ]
            ),
            active=True,
            name=FhirList(
                [
                    HumanName(
                        given=FhirList(
                            [
                                A.column("provider_first_name"),
                                A.column("provider_middle_name"),
                            ]
                        ),
                        family=A.column("provider_last_name"),
                        suffix=FhirList([A.column("provider_title")]),
                        use=NameUseCodeValues.Usual,
                        text=A.text(""),
                    )
                ]
            ),
            # birthdate="",
            gender=AdministrativeGenderCode(
                A.expression(
                    """
                    CASE
                        WHEN `provider_gender` = 'MALE'
                        THEN 'male'
                        WHEN `provider_gender` = 'FEMALE'
                        THEN 'female'
                        ELSE 'unknown'
                    END
                    """
                )
            ),
            telecom=FhirList([]),
            address=FhirList(
                [
                    Address(
                        use=AddressUseCodeValues.Work,  # AddressUseCode.Work,
                        type_=AddressTypeCode(
                            A.text("Physical")
                        ),  # AddressTypeCode.Physical,
                        text=A.column("practice_name"),
                        line=FhirList(
                            [
                                A.column("scheduling_location_address1"),
                                A.column("scheduling_location_address2"),
                            ]
                        ),
                        city=A.column("scheduling_location_city"),
                        district=A.text(""),
                        state=A.column("scheduling_location_state"),
                        postalCode=A.column("scheduling_location_zip"),
                        country=A.text("USA"),
                    )
                ]
            ),
        )
    )
    return mapper
