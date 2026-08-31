"""Canonical public declarations plus an optional private source overlay.

The public declarations contain non-sensitive dataset semantics. Licensed or
confidential source column names belong in a JSON overlay outside the Git
repository. Producers and readers consume the same resolved registry.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from research_store.foundation.models import (
    AnnotationSpec,
    DatasetKind,
    DatasetReadiness,
    DatasetSpec,
    DocumentationSpec,
    Registry,
    SentinelRule,
    StorageModel,
    TemporalKind,
    PlausibleBand,
    VariableSpec,
)

_PROVISIONAL = DatasetReadiness.PROVISIONAL
PRIVATE_REGISTRY_ENV = "RESEARCH_STORE_PRIVATE_REGISTRY"


def _eccc_timezone_entries(
    entities: tuple[str, ...], timezone_name: str, evidence: str
) -> dict[str, dict[str, str]]:
    return {
        entity: {"timezone_name": timezone_name, "evidence": evidence}
        for entity in entities
    }


_ECCC_HOURLY_TIMEZONE_OVERRIDES: dict[str, dict[str, str]] = {
    "1102259": {
        "timezone_name": "America/Vancouver",
        "evidence": (
            "Climate ID 1102259 is absent from the 2025 and 2021 station "
            "inventories. ECCC documents digits 110 as the British Columbia "
            "climatological district; all 314 inventoried district-110 stations "
            "use Pacific standard-time histories (311 America/Vancouver and "
            "three coordinate-boundary America/Los_Angeles assignments), and "
            "the adjacent 1102252-1102257 Cypress Bowl stations are near "
            "49.4 N, 123.2 W."
        ),
    },
    "6112335": {
        "timezone_name": "America/Toronto",
        "evidence": (
            "Climate ID 6112335 occurs in ECCC HLY01_RCS files from 2004 "
            "through 2013 but is absent from both the 2025 station inventory "
            "workbook and ECCC's 2026-08-25 station inventory/API. ECCC "
            "documents the first three Climate ID digits as province and "
            "climatological district; all 187 inventoried district-611 "
            "stations resolve to America/Toronto."
        ),
    },
    "611E001": {
        "timezone_name": "America/Toronto",
        "evidence": (
            "ECCC's archived 2023 station inventory identifies Climate ID "
            "611E001 as EGBERT CS, Ontario, at 44.23 N, 79.78 W; that location "
            "uses America/Toronto standard time."
        ),
    },
    "6158434": {
        "timezone_name": "America/Toronto",
        "evidence": (
            "Climate ID 6158434 is absent from the 2025 and 2021 station "
            "inventories. ECCC documents digits 615 as the Ontario "
            "climatological district, and all 393 inventoried district-615 "
            "stations resolve to America/Toronto."
        ),
    },
    "6158435": {
        "timezone_name": "America/Toronto",
        "evidence": (
            "Climate ID 6158435 is absent from the 2025 and 2021 station "
            "inventories. ECCC documents digits 615 as the Ontario "
            "climatological district, and all 393 inventoried district-615 "
            "stations resolve to America/Toronto."
        ),
    },
    **_eccc_timezone_entries(
        (
            "1013754",
            "1013755",
            "1101562",
            "1103328",
            "110JA54",
            "1118135",
            "1123835",
        ),
        "America/Vancouver",
        (
            "These retired Climate IDs are absent from the 2021 and 2025 "
            "inventories. ECCC defines their first three digits as British "
            "Columbia climatological districts; all inventoried district-101, "
            "111, and 112 stations use America/Vancouver, while district 110 "
            "uses the same Pacific standard offset (311 America/Vancouver and "
            "three boundary-assigned America/Los_Angeles stations)."
        ),
    ),
    **_eccc_timezone_entries(
        ("1148211",),
        "America/Vancouver",
        (
            "Climate ID 1148211 is absent from the 2021 and 2025 inventories; "
            "the immediately adjacent ECCC station 1148212 is TRAIL "
            "SUNNINGDALE, British Columbia (49.13 N, 117.73 W), in "
            "America/Vancouver."
        ),
    ),
    **_eccc_timezone_entries(
        (
            "6143070",
            "6143073",
            "6143074",
            "6143075",
            "6143077",
            "6143087",
        ),
        "America/Toronto",
        (
            "These retired Climate IDs are absent from the 2021 and 2025 "
            "inventories. ECCC defines 614 as their Ontario climatological "
            "district, and all 134 inventoried district-614 stations resolve "
            "to America/Toronto."
        ),
    ),
    **_eccc_timezone_entries(
        ("6155186", "615874R", "61587D9"),
        "America/Toronto",
        (
            "These retired Climate IDs are absent from the 2021 and 2025 "
            "inventories. ECCC defines 615 as their Ontario climatological "
            "district, and all 393 inventoried district-615 stations resolve "
            "to America/Toronto."
        ),
    ),
    **_eccc_timezone_entries(
        ("5040800", "5042260"),
        "America/Winnipeg",
        (
            "These retired Climate IDs are absent from the 2021 and 2025 "
            "inventories. ECCC defines 504 as their Manitoba climatological "
            "district, and all 106 inventoried district-504 stations resolve "
            "to America/Winnipeg."
        ),
    ),
    **_eccc_timezone_entries(
        ("3053255",),
        "America/Edmonton",
        (
            "Climate ID 3053255 is absent from the 2021 and 2025 inventories. "
            "ECCC defines 305 as its Alberta climatological district, and all "
            "168 inventoried district-305 stations resolve to America/Edmonton."
        ),
    ),
    **_eccc_timezone_entries(
        ("8300526",),
        "America/Halifax",
        (
            "Climate ID 8300526 is absent from the 2021 and 2025 inventories. "
            "ECCC defines 830 as its Nova Scotia climatological district, and "
            "all 56 inventoried district-830 stations resolve to America/Halifax."
        ),
    ),
    **_eccc_timezone_entries(
        ("4015340", "4057140", "4068342"),
        "America/Regina",
        (
            "These retired Saskatchewan Climate IDs are absent from the 2021 "
            "and 2025 inventories. District 401 is uniformly America/Regina; "
            "4057140 is bracketed by ECCC's Saskatoon stations 4057130 and "
            "4057152; and 4068342 is adjacent to URANIUM CITY A (4068340), all "
            "in America/Regina."
        ),
    ),
    **_eccc_timezone_entries(
        (
            "7016K95",
            "7032888",
            "7033463",
            "703L67H",
            "70423Q8",
            "7067399",
            "7096625",
        ),
        "America/Toronto",
        (
            "These retired Quebec Climate IDs are absent from the 2021 and "
            "2025 inventories. Districts 701, 703, and 706 are uniformly "
            "America/Toronto; 70423Q8 is bracketed by ECCC's district-704 "
            "Forestville/Foret stations; and 7096625 is bracketed by ECCC's "
            "Riviere Eastmain/Rupert stations, all in America/Toronto."
        ),
    ),
    **_eccc_timezone_entries(
        ("7102919",),
        "America/Toronto",
        (
            "Published Quebec rainfall documentation identifies Climate ID "
            "7102919 as GREAT WHALE RIVER. Its eastern Quebec location and the "
            "district-710 inventory resolve to America/Toronto."
        ),
    ),
    **_eccc_timezone_entries(
        ("8100380", "8101265", "8101600", "8104926", "810N001"),
        "America/Moncton",
        (
            "These retired New Brunswick Climate IDs are absent from the 2021 "
            "and 2025 inventories. ECCC defines 810 as their climatological "
            "district; 228 of 229 inventoried district-810 stations resolve to "
            "America/Moncton, with one polygon-boundary anomaly."
        ),
    ),
}


BASE_REGISTRY = Registry(
    [
        DatasetSpec(
            dataset_id="eccc_hly01_observations",
            description=(
                "ECCC HLY01 hourly weather observations, including RCS elements"
            ),
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="element-specific intervals from 5 minutes to 1 hour",
            source_timezone="station-specific IANA local standard time",
            timestamp_semantics=(
                "HLY01 slots 00-23; each registered element declares its interval "
                "within the source hour and is converted from local standard time"
            ),
            variables=(
                VariableSpec(
                    "precipitation_amount_1h",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_1h_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.02,
                        maximum=20.0,
                        ignore_zeros=True,
                        evidence=(
                            "A tripwire for a wrong scale, not a quality filter. "
                            "Hourly precipitation is zero for about 93 percent of "
                            "slots and this un-QC archive carries a junk block "
                            "above 100 mm that reaches 1.1 percent of values in "
                            "some years, so any quantile of the whole "
                            "distribution is either zero or inside the junk. The "
                            "median of the values that recorded something is "
                            "neither: it reads 0.3-1.1 mm across the delivered "
                            "2004-2025 archive, and a scale wrong by a hundred "
                            "would read 30-110 mm."
                        ),
                    ),
                ),
                VariableSpec(
                    "precipitation_amount_15min",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_15min_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.02,
                        maximum=20.0,
                        ignore_zeros=True,
                        evidence=(
                            "A tripwire for a wrong scale, measured on the values "
                            "that recorded something for the same reason as the "
                            "hourly total. The median non-zero fifteen-minute "
                            "amount reads 0.3-0.6 mm across the delivered "
                            "2004-2025 archive."
                        ),
                    ),
                ),
                VariableSpec(
                    "precipitation_gauge_weight",
                    "precipitation gauge mass per unit area",
                    "kg/m2",
                    quality_field="precipitation_gauge_weight_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.0,
                        maximum=5000.0,
                        evidence=(
                            "A weighing gauge accumulates between servicing "
                            "visits, so its typical reading is bounded by bucket "
                            "capacity rather than by any single storm. The "
                            "observed median is about 238 kg/m2; a scale wrong "
                            "by a hundred would read about 23800."
                        ),
                    ),
                ),
                VariableSpec(
                    "wind_speed_2m_15min",
                    "wind speed at approximately 2 m",
                    "km/h",
                    quality_field="wind_speed_2m_15min_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.5,
                        maximum=40.0,
                        evidence=(
                            "Median near-surface wind speed across a year of "
                            "national records is a light breeze; the delivered "
                            "archive reads about 8-10 km/h. A median outside "
                            "0.5-40 km/h means the scale is wrong, not that the "
                            "weather was."
                        ),
                    ),
                ),
                VariableSpec(
                    "snow_depth",
                    "snow depth",
                    "cm",
                    quality_field="snow_depth_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.99,
                        minimum=0.0,
                        maximum=1200.0,
                        evidence=(
                            "The deepest snow depth ever measured on Earth is "
                            "about 1146 cm (Tamarack, California, 1911), so a "
                            "99th percentile above 1200 cm cannot be snow and "
                            "indicates a wrong scale factor or unit. This is the "
                            "check that would have caught elements 275-278 being "
                            "published at scale 1.0: the 99th percentile read "
                            "about 17500 cm then, and about 175 cm once corrected."
                        ),
                    ),
                ),
                VariableSpec(
                    "wind_direction_2m_10min",
                    "wind direction at approximately 2 m",
                    "degree_true",
                    quality_field="wind_direction_2m_10min_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.99,
                        minimum=0.0,
                        maximum=360.0,
                        evidence="A compass bearing cannot leave [0, 360] degrees.",
                    ),
                ),
                VariableSpec(
                    "wind_speed_2m_10min",
                    "wind speed at approximately 2 m",
                    "km/h",
                    quality_field="wind_speed_2m_10min_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.5,
                        maximum=40.0,
                        evidence=(
                            "Median near-surface wind speed across a year of "
                            "national records is a light breeze; the delivered "
                            "archive reads about 8-10 km/h. A median outside "
                            "0.5-40 km/h means the scale is wrong, not that the "
                            "weather was."
                        ),
                    ),
                ),
            ),
            sentinel_rules=(
                SentinelRule(
                    marker="-99999",
                    meaning="missing",
                    replacement=None,
                    evidence="ECCC Digital Archive Technical Documentation, section 2.3",
                ),
                SentinelRule(
                    marker="######",
                    meaning="missing",
                    replacement=None,
                    evidence=(
                        "Observed verbatim in four element-275 fields in the "
                        "publisher file HLY01_RCS_P2004. ECCC Digital Archive "
                        "Technical Documentation section 2.3 defines the "
                        "six-character value as signed numeric, so these "
                        "hash-filled fields contain no recoverable datum; the "
                        "immutable raw source preserves the original bytes."
                    ),
                ),
            ),
            documentation=DocumentationSpec(
                source_format=(
                    "ECCC HLY fixed-width ASCII: one 186-character "
                    "station/date/element record with 24 signed-value and "
                    "single-character flag fields"
                ),
                references=(
                    "Environment and Climate Change Canada, Technical "
                    "Documentation: Digital Archive of Canadian "
                    "Climatological Data, modified 2017-02-15, sections 1.1, "
                    "2.3, 3.1, Table 19, and Note 33.",
                    "ECCC Applied Climatology Services email, 2025-05-01, "
                    "service request K0414MYP9M / NIRT_0111308.",
                ),
                quality_control=(
                    "Treat this delivered HLY01_RCS archive as raw/un-QC. The "
                    "2025 delivery email explicitly says no quality control "
                    "was done and ECCC has no information about its quality. "
                    "Technical Documentation Note 33 says archive status is R "
                    "before 2013-12-10 and Q after basic automatic ingest "
                    "assessment from that date; that R/Q status is not encoded "
                    "in the supplied fixed-width records, so it is not assigned "
                    "to individual stored values."
                ),
                quality_flags={
                    "blank": "No source flag; generally documented as valid data.",
                    "M": "Missing value; normally paired with -99999.",
                },
                limitations=(
                    "The delivery-specific email provides no endorsed "
                    "scientific-quality assessment for HLY01 values.",
                    "The archive-level R/Q status described in Note 33 cannot "
                    "be reconstructed per value from these files.",
                ),
                notes=(
                    "Elements 275-278 (snow depth) are scaled by 0.01, not 1.0. "
                    "The public Technical Documentation lists snow depth in "
                    "whole centimetres, but the delivered HLY01_RCS bytes "
                    "contradict that: element-275 integers move in steps of 100 "
                    "(0, +/-100, +/-200, 300, ...), the 2005 median is 100 and "
                    "the maximum 92600. Read as whole centimetres that is a one "
                    "metre median and a 926 metre maximum. Read as hundredths of "
                    "a centimetre it is a 1 cm median, a 194 cm 99th percentile "
                    "and a -2 cm sensor noise floor, which also matches the 1 cm "
                    "resolution this repository's own ingestion report states. "
                    "The delivered RCS archive is not the public HLY01 product, "
                    "so the public table is taken as not describing it. Values "
                    "published before 2026-08-31 used scale 1.0 and were a "
                    "hundred times too large; those snapshots were superseded. "
                    "Confirmation has been requested from ECCC Applied "
                    "Climatology Services under service request K0414MYP9M.",
                    "Elements 262-280 use source slots 00-23 in local standard "
                    "time and are stored as canonical half-open UTC intervals.",
                    "Elements 267-270 and 275-278 use a 9-minute computation "
                    "window before 2007 and a 5-minute filtered window from "
                    "2007 onward; elements 279-280 cover minutes 50-60.",
                    "The delivery email describes HLY01 precipitation as "
                    "year-round weighing-gauge data available since about 2004.",
                ),
            ),
            ingest_options={
                "station_slice": [0, 7],
                "date_slice": [7, 15],
                "date_format": "%Y%m%d",
                "element_slice": [15, 18],
                "values_start": 18,
                "field_width": 7,
                "value_width": 6,
                "timezone_policy": "station_inventory",
                "station_dataset_id": "eccc_station_inventory",
                "timezone_overrides": dict(_ECCC_HOURLY_TIMEZONE_OVERRIDES),
                "missing_timezone_policy": "quarantine_record",
                "standard_time_transition_policy": "quarantine_record",
                "elements": {
                    "262": {
                        "variable": "precipitation_amount_1h",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 60,
                    },
                    "263": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 15,
                    },
                    "264": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 15,
                        "duration_minutes": 15,
                    },
                    "265": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 30,
                        "duration_minutes": 15,
                    },
                    "266": {
                        "variable": "precipitation_amount_15min",
                        "scale": 0.1,
                        "start_minute": 45,
                        "duration_minutes": 15,
                    },
                    "267": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 15,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "268": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 30,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "269": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 45,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "270": {
                        "variable": "precipitation_gauge_weight",
                        "scale": 0.1,
                        "end_minute": 60,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "271": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 15,
                    },
                    "272": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 15,
                        "duration_minutes": 15,
                    },
                    "273": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 30,
                        "duration_minutes": 15,
                    },
                    "274": {
                        "variable": "wind_speed_2m_15min",
                        "scale": 0.1,
                        "start_minute": 45,
                        "duration_minutes": 15,
                    },
                    "275": {
                        "variable": "snow_depth",
                        # The delivered RCS bytes are hundredths of a centimetre:
                        # raw values move in steps of 100 and a 1 cm sensor
                        # resolution therefore requires this scale. See the
                        # documentation note on the unit conflict below.
                        "scale": 0.01,
                        "end_minute": 60,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "276": {
                        "variable": "snow_depth",
                        # The delivered RCS bytes are hundredths of a centimetre:
                        # raw values move in steps of 100 and a 1 cm sensor
                        # resolution therefore requires this scale. See the
                        # documentation note on the unit conflict below.
                        "scale": 0.01,
                        "end_minute": 15,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "277": {
                        "variable": "snow_depth",
                        # The delivered RCS bytes are hundredths of a centimetre:
                        # raw values move in steps of 100 and a 1 cm sensor
                        # resolution therefore requires this scale. See the
                        # documentation note on the unit conflict below.
                        "scale": 0.01,
                        "end_minute": 30,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "278": {
                        "variable": "snow_depth",
                        # The delivered RCS bytes are hundredths of a centimetre:
                        # raw values move in steps of 100 and a 1 cm sensor
                        # resolution therefore requires this scale. See the
                        # documentation note on the unit conflict below.
                        "scale": 0.01,
                        "end_minute": 45,
                        "duration_minutes": 5,
                        "before_year": 2007,
                        "duration_minutes_before": 9,
                    },
                    "279": {
                        "variable": "wind_direction_2m_10min",
                        "scale": 1.0,
                        "start_minute": 50,
                        "duration_minutes": 10,
                    },
                    "280": {
                        "variable": "wind_speed_2m_10min",
                        "scale": 0.1,
                        "start_minute": 50,
                        "duration_minutes": 10,
                    },
                },
                "encoding": "ascii",
            },
        ),
        DatasetSpec(
            dataset_id="eccc_hly03_observations",
            description="ECCC HLY03 hourly rainfall rate archive (element 123)",
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_hourly",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            source_timezone="station-specific IANA local standard time",
            timestamp_semantics=(
                "source slots are hourly intervals ending 01-24 local standard time"
            ),
            variables=(
                VariableSpec(
                    "precipitation_amount_1h",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_1h_quality",
                ),
            ),
            sentinel_rules=(
                SentinelRule(
                    marker="-99999",
                    meaning="missing",
                    replacement=None,
                    evidence="ECCC Digital Archive Technical Documentation, section 2.3",
                ),
            ),
            documentation=DocumentationSpec(
                source_format=(
                    "ECCC HLY fixed-width ASCII: one 186-character "
                    "station/date/element record with 24 signed-value and "
                    "single-character flag fields"
                ),
                references=(
                    "Environment and Climate Change Canada, Technical "
                    "Documentation: Digital Archive of Canadian "
                    "Climatological Data, modified 2017-02-15, sections 1.1, "
                    "2.3, 3.2, and Table 19.",
                    "ECCC Applied Climatology Services email, 2025-05-01, "
                    "service request K0414MYP9M / NIRT_0111308.",
                ),
                quality_control=(
                    "ECCC describes HLY03 element 123 in this delivery as "
                    "quality controlled. Source flags and unexpected flag "
                    "symbols are preserved verbatim so users can apply "
                    "analysis-specific acceptance rules."
                ),
                quality_flags={
                    "blank": "Valid data.",
                    "H": "Freezing precipitation.",
                    "I": "Unadjusted value.",
                    "J": "Freezing and unadjusted value.",
                    "M": "Missing value; normally paired with -99999.",
                },
                limitations=(
                    "The tipping-bucket gauge is normally available during the "
                    "warm season, approximately April through October.",
                    "A winter zero can mean that the gauge was frozen or snow "
                    "covered and must not automatically be interpreted as no "
                    "precipitation.",
                    "Tipping-bucket gauges require caution for solid "
                    "precipitation.",
                ),
                notes=(
                    "Element 123 has 0.1 mm archived resolution; the delivery "
                    "email describes a 0.2 mm tipping-bucket tip mechanism.",
                    "Source slots end at hours 01-24 local standard time. The "
                    "store represents the same hourly ending boundaries as "
                    "canonical half-open UTC intervals.",
                ),
            ),
            ingest_options={
                "station_slice": [0, 7],
                "date_slice": [7, 15],
                "date_format": "%Y%m%d",
                "element_slice": [15, 18],
                "values_start": 18,
                "field_width": 7,
                "value_width": 6,
                "timezone_policy": "station_inventory",
                "station_dataset_id": "eccc_station_inventory",
                "timezone_overrides": dict(_ECCC_HOURLY_TIMEZONE_OVERRIDES),
                "missing_timezone_policy": "quarantine_record",
                "malformed_record_policy": "salvage_valid_fixed_width_records",
                "standard_time_transition_policy": "quarantine_record",
                "elements": {
                    "123": {
                        "variable": "precipitation_amount_1h",
                        "scale": 0.1,
                        "start_minute": 0,
                        "duration_minutes": 60,
                    }
                },
                "encoding": "ascii",
            },
        ),
        DatasetSpec(
            dataset_id="eccc_dly04_observations",
            description=(
                "ECCC DLY04 daily climatological observations: temperature and "
                "precipitation elements on the climatological day"
            ),
            kind=DatasetKind.EXTERNAL,
            producer="fixed_width_daily",
            storage_model=StorageModel.LONG,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 day",
            source_timezone="UTC",
            timestamp_semantics=(
                "one 233-character record holds one station, month and element "
                "followed by 31 day slots; a day slot is the climatological day, "
                "which for stations operating on a 24 hour basis ends at 0600Z of "
                "the following day, so the stored interval is [day 06:00Z, "
                "day+1 06:00Z)"
            ),
            variables=(
                VariableSpec(
                    "air_temperature_max",
                    "maximum air temperature",
                    "degC",
                    quality_field="air_temperature_max_quality",
                ),
                VariableSpec(
                    "air_temperature_min",
                    "minimum air temperature",
                    "degC",
                    quality_field="air_temperature_min_quality",
                ),
                VariableSpec(
                    "air_temperature_mean",
                    "mean air temperature",
                    "degC",
                    quality_field="air_temperature_mean_quality",
                ),
                VariableSpec(
                    "rainfall_amount",
                    "rainfall amount",
                    "mm",
                    quality_field="rainfall_amount_quality",
                ),
                VariableSpec(
                    "snowfall_amount",
                    "snowfall amount",
                    "cm",
                    quality_field="snowfall_amount_quality",
                ),
                VariableSpec(
                    "precipitation_amount",
                    "precipitation amount",
                    "mm",
                    quality_field="precipitation_amount_quality",
                ),
            ),
            sentinel_rules=(
                SentinelRule(
                    marker="-99999",
                    meaning="missing",
                    replacement=None,
                    evidence=(
                        "ECCC Digital Archive Technical Documentation, section 2.3: "
                        "value fields are initialized to -99999M"
                    ),
                ),
            ),
            ingest_options={
                "station_slice": [0, 7],
                "date_slice": [7, 13],
                "date_format": "%Y%m",
                "element_slice": [13, 16],
                "values_start": 16,
                "field_width": 7,
                "value_width": 6,
                "day_slots": 31,
                # Stations reporting at morning and afternoon observation times
                # close their day at station-specific clock times that only the
                # historical inspection reports carry, so the class is declared
                # here and the ingester refuses any other value.
                "station_day_class": "synoptic_24h",
                # Inclusive start, exclusive end. An uncovered era fails rather
                # than inheriting the modern boundary: before 1961-07-01 the
                # maximum and minimum temperature days closed at different hours,
                # so those records need their own element-aware declaration.
                "climatological_day_rules": [
                    {
                        "start": "1961-07-01",
                        "end": None,
                        "utc_end_hour": 6,
                        "evidence": (
                            "ECCC Digital Archive Technical Documentation: for "
                            "climate stations operating on a 24 hour basis, since "
                            "July 1, 1961, the climatological day for temperature "
                            "and precipitation ends at 0600Z of the following day"
                        ),
                    },
                ],
                # Snow on the ground (element 013) is deliberately absent: the
                # archive documents no observation time for it, so it cannot be
                # given an interval without guessing.
                "elements": {
                    "001": {"variable": "air_temperature_max", "scale": 0.1},
                    "002": {"variable": "air_temperature_min", "scale": 0.1},
                    "003": {"variable": "air_temperature_mean", "scale": 0.1},
                    "010": {"variable": "rainfall_amount", "scale": 0.1},
                    "011": {"variable": "snowfall_amount", "scale": 0.1},
                    "012": {"variable": "precipitation_amount", "scale": 0.1},
                },
                "encoding": "ascii",
            },
        ),
        DatasetSpec(
            dataset_id="hydrometric_discharge_unit_corrected",
            description=(
                "Corrected instantaneous discharge unit values from the ECCC "
                "national hydrometric publisher export"
            ),
            kind=DatasetKind.EXTERNAL,
            producer="unit_value_corrected",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INSTANT,
            time_end_field=None,
            native_frequency=None,
            source_timezone="UTC",
            timestamp_semantics=(
                "publisher ISO 8601 UTC observation timestamp; instantaneous "
                "values retain their native, potentially changing cadence"
            ),
            snapshot_mode="replace",
            variables=(
                VariableSpec("discharge", "volumetric flow rate", "m3/s"),
            ),
            annotations=(
                AnnotationSpec(
                    "approval_level",
                    "publisher AQUARIUS approval level retained verbatim",
                ),
                AnnotationSpec(
                    "grade",
                    "publisher unit-value grade code retained verbatim",
                ),
                AnnotationSpec(
                    "qualifiers",
                    "publisher unit-value qualifiers retained as a JSON string array",
                ),
            ),
            documentation=DocumentationSpec(
                source_format=(
                    "XZ-compressed AQUARIUS CSV with a comment metadata header "
                    "and the columns ISO 8601 UTC, local fixed-offset timestamp, "
                    "Value, Approval Level, Grade and Qualifiers"
                ),
                references=(
                    "Environment and Climate Change Canada, Unit Value Data "
                    "Disclaimer, accessed 2026-08-31.",
                    "Environment and Climate Change Canada, Hydrometric Unit "
                    "Value Data corrected discharge directory, snapshot "
                    "collected 2026-08-31.",
                ),
                quality_control=(
                    "The corrected signal is retained exactly as published. "
                    "Approval Level, Grade and Qualifiers are stored as separate "
                    "annotations; ingestion does not discard observations based "
                    "on those fields. The publisher can emit multiple trailing "
                    "qualifier fields despite one Qualifiers header; all are "
                    "retained in order as a JSON string array. Unit values remain "
                    "preliminary and can be revised by the publisher."
                ),
                quality_flags={
                    "-2": "Unusable.",
                    "-1": "Unspecified.",
                    "0": "Undefined.",
                    "10": "Ice.",
                    "20": "Estimated.",
                    "30": "Partial day.",
                    "40": "Dry.",
                    "50": "Revised.",
                },
                limitations=(
                    "Unit values are preliminary and are not the official "
                    "archived hydrometric record.",
                    "Sampling cadence varies between stations and can change "
                    "within one station record.",
                ),
                notes=(
                    "The explicit UTC timestamp is canonical; the redundant "
                    "fixed-offset local timestamp is not republished.",
                    "Optional drainage-area selection is an ingestion-run "
                    "parameter and is not part of this dataset's identity.",
                ),
            ),
            ingest_options={
                "manifest_delimiter": "\t",
                "manifest_columns": [
                    "region",
                    "filename",
                    "publisher_modified",
                    "publisher_listed_size",
                    "source_url",
                ],
                "station_table": "STATIONS",
                "station_id_column": "STATION_NUMBER",
                "drainage_area_column": "DRAINAGE_AREA_GROSS",
                "station_metadata_uri": (
                    "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
                    "Hydat_sqlite3_20260717.zip#Hydat.sqlite3"
                ),
            },
        ),
        DatasetSpec(
            dataset_id="hydrometric_flow_daily",
            description="Daily discharge values unpivoted from the national SQLite archive",
            kind=DatasetKind.EXTERNAL,
            producer="hydrometric_sqlite",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 day",
            snapshot_mode="replace",
            variables=(
                VariableSpec(
                    "discharge",
                    "volumetric flow rate",
                    "m3/s",
                    quality_field="discharge_quality",
                ),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "table": None,
                "station_column": None,
                "year_column": None,
                "month_column": None,
                "value_prefix": None,
                "quality_prefix": None,
            },
            unresolved_decisions=(
                "confirm SQLite table and column names",
                "record quality-symbol meanings and source daily-time semantics",
            ),
        ),
        DatasetSpec(
            dataset_id="hydrometric_level_daily",
            description="Daily stage values unpivoted from the national SQLite archive",
            kind=DatasetKind.EXTERNAL,
            producer="hydrometric_sqlite",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 day",
            snapshot_mode="replace",
            variables=(
                VariableSpec(
                    "stage", "water level", "m", quality_field="stage_quality"
                ),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "table": None,
                "station_column": None,
                "year_column": None,
                "month_column": None,
                "value_prefix": None,
                "quality_prefix": None,
            },
            unresolved_decisions=(
                "confirm SQLite table and column names",
                "record quality-symbol meanings and source daily-time semantics",
            ),
        ),
        DatasetSpec(
            dataset_id="eccc_station_inventory",
            description="Versioned ECCC station inventory workbook",
            kind=DatasetKind.REFERENCE,
            producer="inventory_csv",
            storage_model=StorageModel.REFERENCE,
            temporal_kind=TemporalKind.REFERENCE,
            time_start_field=None,
            time_end_field=None,
            canonical_timezone=None,
            snapshot_mode="replace",
            variables=(
                VariableSpec("station_name", "station name", None, dtype="string"),
                VariableSpec("province", "province or territory", None, dtype="string"),
                VariableSpec(
                    "source_station_id",
                    "publisher internal station identifier",
                    None,
                    dtype="string",
                ),
                VariableSpec("wmo_id", "WMO identifier", None, dtype="string"),
                VariableSpec(
                    "tc_id", "Transport Canada identifier", None, dtype="string"
                ),
                VariableSpec("latitude", "latitude", "degree_north"),
                VariableSpec("longitude", "longitude", "degree_east"),
                VariableSpec("elevation", "height above reference datum", "m"),
                VariableSpec("first_year", "first year of record", "year"),
                VariableSpec("last_year", "last year of record", "year"),
                VariableSpec("hly_first_year", "first hourly-data year", "year"),
                VariableSpec("hly_last_year", "last hourly-data year", "year"),
                VariableSpec("dly_first_year", "first daily-data year", "year"),
                VariableSpec("dly_last_year", "last daily-data year", "year"),
                VariableSpec("mly_first_year", "first monthly-data year", "year"),
                VariableSpec("mly_last_year", "last monthly-data year", "year"),
                VariableSpec(
                    "timezone_name",
                    "IANA timezone inferred from station coordinates",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "timezone_source",
                    "timezone boundary dataset used for coordinate lookup",
                    None,
                    dtype="string",
                ),
            ),
            partition_keys=(),
            ingest_options={
                "format": "xlsx",
                "header_identifier": "Climate ID",
                "column_map": {
                    "entity_id": "Climate ID",
                    "station_name": "Name",
                    "province": "Province",
                    "source_station_id": "Station ID",
                    "wmo_id": "WMO ID",
                    "tc_id": "TC ID",
                    "latitude": "Latitude (Decimal Degrees)",
                    "longitude": "Longitude (Decimal Degrees)",
                    "elevation": "Elevation (m)",
                    "first_year": "First Year",
                    "last_year": "Last Year",
                    "hly_first_year": "HLY First Year",
                    "hly_last_year": "HLY Last Year",
                    "dly_first_year": "DLY First Year",
                    "dly_last_year": "DLY Last Year",
                    "mly_first_year": "MLY First Year",
                    "mly_last_year": "MLY Last Year",
                },
                "source_longitude_convention": "signed",
                "derive_timezone_from_coordinates": True,
            },
        ),
        DatasetSpec(
            dataset_id="hydrometric_station_inventory",
            description="National hydrometric station inventory extracted from HYDAT STATIONS table",
            kind=DatasetKind.REFERENCE,
            producer="inventory_sqlite",
            storage_model=StorageModel.REFERENCE,
            temporal_kind=TemporalKind.REFERENCE,
            time_start_field=None,
            time_end_field=None,
            canonical_timezone=None,
            snapshot_mode="replace",
            variables=(
                VariableSpec("station_name", "station name", None, dtype="string"),
                VariableSpec(
                    "province",
                    "province, territory or state location",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "regional_office_id",
                    "regional office identifier",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "hyd_status",
                    "hydrometric operational status (A=Active, D=Discontinued)",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "sed_status",
                    "sediment operational status",
                    None,
                    dtype="string",
                ),
                VariableSpec("latitude", "latitude", "degree_north"),
                VariableSpec("longitude", "longitude", "degree_east"),
                VariableSpec("drainage_area_gross", "gross drainage area", "km2"),
                VariableSpec(
                    "drainage_area_effective", "effective drainage area", "km2"
                ),
                VariableSpec(
                    "is_rhbn",
                    "Reference Hydrometric Basin Network indicator",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "is_real_time",
                    "real-time data transmission indicator",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "contributor_id",
                    "data contributor identifier",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "operator_id",
                    "data operator identifier",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "datum_id", "reference datum identifier", None, dtype="string"
                ),
                VariableSpec(
                    "timezone_name",
                    "IANA timezone inferred from station coordinates",
                    None,
                    dtype="string",
                ),
                VariableSpec(
                    "timezone_source",
                    "timezone boundary dataset used for coordinate lookup",
                    None,
                    dtype="string",
                ),
            ),
            partition_keys=(),
            ingest_options={
                "table": "STATIONS",
                "column_map": {
                    "entity_id": "STATION_NUMBER",
                    "station_name": "STATION_NAME",
                    "province": "PROV_TERR_STATE_LOC",
                    "regional_office_id": "REGIONAL_OFFICE_ID",
                    "hyd_status": "HYD_STATUS",
                    "sed_status": "SED_STATUS",
                    "latitude": "LATITUDE",
                    "longitude": "LONGITUDE",
                    "drainage_area_gross": "DRAINAGE_AREA_GROSS",
                    "drainage_area_effective": "DRAINAGE_AREA_EFFECT",
                    "is_rhbn": "RHBN",
                    "is_real_time": "REAL_TIME",
                    "contributor_id": "CONTRIBUTOR_ID",
                    "operator_id": "OPERATOR_ID",
                    "datum_id": "DATUM_ID",
                },
                "source_longitude_convention": "signed",
                "derive_timezone_from_coordinates": True,
            },
        ),
        DatasetSpec(
            dataset_id="reanalysis_points_hourly",
            description="Hourly reanalysis extracted from a rotated-pole grid at approved targets",
            kind=DatasetKind.EXTERNAL,
            producer="reanalysis_netcdf",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            variables=(),
            readiness=_PROVISIONAL,
            ingest_options={
                "sampling_method": None,
                "target_registry": None,
                "variable_map": {},
            },
            unresolved_decisions=(
                "declare variables, units, and NetCDF coordinate names",
                "choose point sampling or area aggregation and preserve its evidence",
                "approve the target point or polygon registry",
            ),
        ),
        DatasetSpec(
            dataset_id="wind_scada_10min",
            description="Offshore turbine SCADA in its naturally dense wide representation",
            kind=DatasetKind.EXTERNAL,
            producer="scada_wide",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="10 minutes",
            source_timezone=None,
            variables=(
                VariableSpec("power", "active power", None),
                VariableSpec("wind_speed", "wind speed", None),
            ),
            readiness=_PROVISIONAL,
            ingest_options={
                "format": None,
                "entity_column": None,
                "timestamp_column": None,
                "column_map": {},
            },
            unresolved_decisions=(
                "confirm file format and all approximately 34 columns with units",
                "record timestamp timezone, interval labelling, and DST behaviour",
                "record meanings of missing, invalid, and not-installed sensor fields",
            ),
        ),
    ]
)


def _overlay_spec(spec: DatasetSpec, payload: dict[str, Any]) -> DatasetSpec:
    if not isinstance(payload, dict):
        raise TypeError(
            f"Private registry entry for {spec.dataset_id} must be an object"
        )
    allowed = {
        "description",
        "source_timezone",
        "timestamp_semantics",
        "readiness",
        "snapshot_mode",
        "variables",
        "ingest_options",
        "unresolved_decisions",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(
            f"Unsupported private registry fields for {spec.dataset_id}: "
            f"{sorted(unknown)}"
        )
    changes: dict[str, Any] = dict(payload)
    if "readiness" in changes:
        changes["readiness"] = DatasetReadiness(str(changes["readiness"]))
    if "variables" in changes:
        variables = changes["variables"]
        if not isinstance(variables, list):
            raise TypeError(f"variables for {spec.dataset_id} must be a list")
        changes["variables"] = tuple(VariableSpec(**item) for item in variables)
    if "unresolved_decisions" in changes:
        changes["unresolved_decisions"] = tuple(changes["unresolved_decisions"])
    if "ingest_options" in changes:
        private_options = changes["ingest_options"]
        if not isinstance(private_options, dict):
            raise TypeError(f"ingest_options for {spec.dataset_id} must be an object")
        changes["ingest_options"] = {
            **dict(spec.ingest_options),
            **private_options,
        }
    resolved = replace(spec, **changes)
    if resolved.readiness is DatasetReadiness.READY and resolved.unresolved_decisions:
        raise ValueError(
            f"Ready private dataset {spec.dataset_id!r} still has unresolved decisions"
        )
    return resolved


def load_registry(private_config: str | Path | None = None) -> Registry:
    """Resolve the public registry with an optional non-versioned JSON overlay."""

    selected = private_config or os.environ.get(PRIVATE_REGISTRY_ENV)
    if not selected:
        return BASE_REGISTRY
    path = Path(selected).expanduser().resolve(strict=True)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Private registry root must be a JSON object")
    unknown_ids = set(payload) - {spec.dataset_id for spec in BASE_REGISTRY}
    if unknown_ids:
        raise KeyError(
            f"Private registry contains unknown datasets: {sorted(unknown_ids)}"
        )
    return Registry(
        _overlay_spec(spec, payload.get(spec.dataset_id, {})) for spec in BASE_REGISTRY
    )


DEFAULT_REGISTRY = load_registry()
