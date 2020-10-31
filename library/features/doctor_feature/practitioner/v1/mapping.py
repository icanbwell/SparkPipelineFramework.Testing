from typing import Any, Dict

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper_fhir.complex_types.address import Address
from spark_auto_mapper_fhir.complex_types.codeableConcept import CodeableConcept
from spark_auto_mapper_fhir.complex_types.coding import Coding
from spark_auto_mapper_fhir.complex_types.human_name import HumanName
from spark_auto_mapper_fhir.complex_types.identifier import Identifier
from spark_auto_mapper_fhir.fhir_types.list import FhirList
from spark_auto_mapper_fhir.resources.practitioner import Practitioner
from spark_auto_mapper_fhir.valuesets.administrative_gender import AdministrativeGenderCode
from spark_auto_mapper_fhir.valuesets.identifier_type import IdentifierTypeCode
from spark_auto_mapper_fhir.valuesets.identifier_use import IdentifierUseCode
from spark_auto_mapper_fhir.valuesets.name_use import NameUseCode


def mapping(parameters: Dict[str, Any]) -> AutoMapper:
    mapper = AutoMapper(
        view=parameters["view"],
        source_view="bwellProviderFeed_08122020",  # file name
        keys=["gecb_provider_number"]
    ).complex(
        Practitioner(
            id_=A.column("b.gecb_provider_number"),
            identifier=FhirList(
                [
                    Identifier(
                        use=IdentifierUseCode.Usual,
                        value=A.column("b.gecb_provider_number"),
                        type_=CodeableConcept(
                            coding=Coding(
                                system=IdentifierTypeCode.codeset,
                                code=IdentifierTypeCode.ProviderNumber
                            )
                        ),
                        system="medstarhealth.org"
                    ),
                    Identifier(
                        use=IdentifierUseCode.Official,
                        value=A.column("provider_npi"),
                        type_=CodeableConcept(
                            coding=Coding(
                                system=IdentifierTypeCode.codeset,
                                code=IdentifierTypeCode.
                                NationalProviderIdentifier
                            )
                        ),
                        system="http://hl7.org/fhir/sid/us-npi"
                    )
                ]
            ),
            active=True,
            name=FhirList(
                [
                    HumanName(
                        given=FhirList(
                            [
                                A.column("provider_first_name"),
                                A.column("provider_middle_name")
                            ]
                        ),
                        family=A.column("provider_last_name"),
                        suffix=FhirList([A.column("provider_title")]),
                        use=NameUseCode.usual,
                        text=A.text("")
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
                        use=A.text("Work"),  # AddressUseCode.Work,
                        type_=A.text("Physical"),  # AddressTypeCode.Physical,
                        text=A.column("practice_name"),
                        line=FhirList(
                            [
                                A.column("scheduling_location_address1"),
                                A.column("scheduling_location_address2")
                            ]
                        ),
                        city=A.column("scheduling_location_city"),
                        district=A.text(""),
                        state=A.column("scheduling_location_state"),
                        postalCode=A.column("scheduling_location_zip"),
                        country=A.text("USA")
                    )
                ]
            )
        )
    )
    return mapper
