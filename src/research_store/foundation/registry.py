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


# The fixed header of an MSC GeoMet climate-hourly `f=csv` response, in the
# order pygeoapi 0.20.0 serves it. A change in names or order stops ingestion.
_GEOMET_CLIMATE_HOURLY_COLUMNS: tuple[str, ...] = (
    "x",
    "y",
    "STATION_NAME",
    "CLIMATE_IDENTIFIER",
    "ID",
    "LOCAL_DATE",
    "PROVINCE_CODE",
    "LOCAL_YEAR",
    "LOCAL_MONTH",
    "LOCAL_DAY",
    "LOCAL_HOUR",
    "UTC_DATE",
    "UTC_YEAR",
    "UTC_MONTH",
    "UTC_DAY",
    "TEMP",
    "TEMP_FLAG",
    "DEW_POINT_TEMP",
    "DEW_POINT_TEMP_FLAG",
    "HUMIDEX",
    "HUMIDEX_FLAG",
    "PRECIP_AMOUNT",
    "PRECIP_AMOUNT_FLAG",
    "RELATIVE_HUMIDITY",
    "RELATIVE_HUMIDITY_FLAG",
    "STATION_PRESSURE",
    "STATION_PRESSURE_FLAG",
    "VISIBILITY",
    "VISIBILITY_FLAG",
    "WEATHER_ENG_DESC",
    "WEATHER_FRE_DESC",
    "WINDCHILL",
    "WINDCHILL_FLAG",
    "WIND_DIRECTION",
    "WIND_DIRECTION_FLAG",
    "WIND_SPEED",
    "WIND_SPEED_FLAG",
    "STN_ID",
    "LONGITUDE_DECIMAL_DEGREES",
    "LATITUDE_DECIMAL_DEGREES",
    "FLAG",
)


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
                    "Stored one hour late (evidence of 2026-09-25, pending a "
                    "controlled replacement): source slot H holds the hour ENDING "
                    "at H LST, but the element declarations place it at [H, H+1). "
                    "At HAMILTON RBG CS (6153301) element 262 is non-zero one "
                    "stored hour after 341 of 388 onsets of "
                    "eccc_hly03_observations element 123, whose slots ECCC "
                    "documents as hours ending 01-24, and at the same stored "
                    "hour after 13; and it equals, slot H for LOCAL_DATE H, the "
                    "MSC GeoMet climate-hourly total that the relative-humidity "
                    "and temperature readings place in the hour ending at H. "
                    "Elements 263-280 share the slot and need the same review. "
                    "Until a registry change, an ingester version bump and a "
                    "supersede-and-re-ingest correct it, subtract one hour from "
                    "HLY01 keys before joining them with eccc_hly03_observations "
                    "or eccc_climate_hourly_observations.",
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
            dataset_id="eccc_climate_hourly_observations",
            description=(
                "ECCC hourly climate observations (temperature, humidity, "
                "precipitation, wind, visibility, pressure, humidex, wind chill, "
                "weather) from the MSC GeoMet OGC API climate-hourly collection"
            ),
            kind=DatasetKind.EXTERNAL,
            producer="geomet_climate_hourly",
            storage_model=StorageModel.WIDE,
            temporal_kind=TemporalKind.INTERVAL,
            native_frequency="1 hour",
            source_timezone="station-specific IANA local standard time",
            timestamp_semantics=(
                "one row per Climate ID and LOCAL_DATE H, the observation time in "
                "local standard time (no daylight saving); the row is keyed by the "
                "hour ending then, [H-1h, H) LST, converted to UTC with the "
                "station's historical standard offset, so time_end is the "
                "publisher's UTC_DATE. precipitation_amount_1h is the total over "
                "that hour; every other variable is observed at its end, H (wind "
                "as the mean of the 1, 2 or 10 minutes ending then). H falls on "
                "the hour, or on the half hour at UTC-3:30 (Newfoundland) "
                "stations. ingest_options.variable_timing declares each placement"
            ),
            snapshot_mode="replace",
            variables=(
                VariableSpec(
                    "air_temperature",
                    "air temperature",
                    "degC",
                    quality_field="air_temperature_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=-50.0,
                        maximum=40.0,
                        evidence=(
                            "No Canadian station has a monthly mean colder than "
                            "about -38 degC (Eureka, February) or warmer than about "
                            "25 degC, so the median of any station-month selection "
                            "lies inside [-50, 40]; HAMILTON RBG CS reads 21.1 degC "
                            "in July 2018 and -0.7 degC in January 2021. The band "
                            "refuses kelvin (about 250-300) and a tenfold error "
                            "wherever the true median is further than about 4.4 "
                            "degC from zero; a tenths error in mild weather passes."
                        ),
                    ),
                ),
                VariableSpec(
                    "dew_point_temperature",
                    "dew point temperature",
                    "degC",
                    quality_field="dew_point_temperature_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=-60.0,
                        maximum=35.0,
                        evidence=(
                            "The dew point cannot exceed the air temperature, and "
                            "Canadian record dew points are below 35 degC, so a "
                            "median outside [-60, 35] degC is a unit or scale "
                            "error. It refuses kelvin; like air temperature it "
                            "cannot see a tenths error near 0 degC."
                        ),
                    ),
                ),
                VariableSpec(
                    "relative_humidity",
                    "relative humidity",
                    "%",
                    quality_field="relative_humidity_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=10.0,
                        maximum=100.0,
                        evidence=(
                            "Relative humidity is bounded by 100 percent and its "
                            "hourly median is well above 10 percent at every "
                            "Canadian station (HAMILTON RBG CS reads about 70-85 "
                            "percent). A fraction (0-1) or a tenfold scale leaves "
                            "the band."
                        ),
                    ),
                ),
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
                            "The same tripwire as eccc_hly01_observations element "
                            "262, which this field is: hourly totals are zero in "
                            "most slots, so only the median of the values that "
                            "recorded something moves with the scale. It reads "
                            "1.05 mm (July 2018) and 0.4 mm (January 2021) at "
                            "HAMILTON RBG CS; a hundredfold error would read 40-105."
                        ),
                    ),
                ),
                VariableSpec(
                    "wind_direction",
                    (
                        "wind direction, degrees true from which the wind blows "
                        "(360 is north; null when the publisher reports calm)"
                    ),
                    "degree_true",
                    quality_field="wind_direction_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.99,
                        minimum=0.0,
                        maximum=360.0,
                        evidence=(
                            "A compass bearing cannot leave [0, 360] degrees. The "
                            "publisher's tens-of-degrees encoding is checked value "
                            "by value instead (source_integer_ranges: integers "
                            "0-36), which, unlike a quantile, does not depend on "
                            "how much data or which wind regime a selection holds."
                        ),
                    ),
                ),
                VariableSpec(
                    "wind_speed",
                    (
                        "wind speed, usually at 10 m, averaged over the 1, 2 or "
                        "10 minutes ending at the observation time"
                    ),
                    "km/h",
                    quality_field="wind_speed_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.5,
                        maximum=40.0,
                        ignore_zeros=True,
                        evidence=(
                            "Calm hours are reported as 0 (33 percent of July 2018 "
                            "and 22 percent of January 2021 at HAMILTON RBG CS), so "
                            "the band measures non-calm hours, whose median is a "
                            "light breeze: 6-7 km/h there, and about 8-10 km/h "
                            "across the HLY archive. m/s or knots stay inside; a "
                            "tenfold scale does not."
                        ),
                    ),
                ),
                VariableSpec(
                    "visibility",
                    "horizontal visibility",
                    "km",
                    quality_field="visibility_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=0.5,
                        maximum=100.0,
                        evidence=(
                            "Staffed stations report visibility in kilometres and "
                            "their hourly median is tens of kilometres; metres "
                            "would read thousands. Automatic stations such as "
                            "HAMILTON RBG CS report none, and an all-null "
                            "publication is not measured."
                        ),
                    ),
                ),
                VariableSpec(
                    "station_pressure",
                    "air pressure at station elevation",
                    "kPa",
                    quality_field="station_pressure_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=50.0,
                        maximum=110.0,
                        evidence=(
                            "Station pressure is about 101 kPa at sea level and "
                            "about 76 kPa at the highest Canadian stations (about "
                            "2300 m); HAMILTON RBG CS reads 100.4-100.6 kPa. The "
                            "band refuses hPa (about 1000) and Pa."
                        ),
                    ),
                ),
                VariableSpec(
                    "humidex",
                    "humidex index",
                    "1",
                    quality_field="humidex_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=20.0,
                        maximum=60.0,
                        evidence=(
                            "ECCC displays humidex only when the air temperature is "
                            "at least 20 degC and humidex exceeds it by at least 1, "
                            "so every published value is at least 21; the Canadian "
                            "record is about 53. The July 2018 median at HAMILTON "
                            "RBG CS is 29."
                        ),
                    ),
                ),
                VariableSpec(
                    "wind_chill",
                    "wind chill index",
                    "1",
                    quality_field="wind_chill_quality",
                    plausible_band=PlausibleBand(
                        quantile=0.5,
                        minimum=-70.0,
                        maximum=5.0,
                        evidence=(
                            "Wind chill is computed only when the air temperature "
                            "is at or below 0 degC, so its median is negative; the "
                            "January 2021 median at HAMILTON RBG CS is -6. The band "
                            "catches a sign flip and a hundredfold scale."
                        ),
                    ),
                ),
                VariableSpec(
                    "weather_description",
                    "present weather description (English)",
                    None,
                    dtype="string",
                ),
            ),
            annotations=(
                AnnotationSpec(
                    "source_station_id",
                    "publisher STN_ID internal station index retained verbatim",
                ),
                AnnotationSpec(
                    "record_flag",
                    (
                        "publisher record-level FLAG field retained verbatim; its "
                        "codes are not documented by the collection"
                    ),
                ),
            ),
            # Each marker applies only to the value fields that
            # ingest_options.sentinel_variables names for it.
            sentinel_rules=(
                SentinelRule(
                    marker="",
                    meaning="missing",
                    replacement=None,
                    evidence=(
                        "GeoMet f=csv writes a null property as an empty field; the "
                        "same records in f=json carry null (HAMILTON RBG CS "
                        "2024-07-01, retrieved 2026-09-25); the ECCC bulk hourly "
                        "CSV leaves these values blank. Applied to every variable "
                        "value field; flags and annotations keep a blank verbatim."
                    ),
                ),
                SentinelRule(
                    marker="NA",
                    meaning="missing",
                    replacement=None,
                    evidence=(
                        "WEATHER_ENG_DESC is 'NA' with WEATHER_FRE_DESC 'ND' (non "
                        "disponible) in every hour of 2018-07 and 2021-01 at "
                        "HAMILTON RBG CS, a station reporting no present weather; "
                        "the bulk hourly CSV writes 'NA' or blank for the same "
                        "hours. Scoped to weather_description only: nothing shows "
                        "'NA' in a numeric field, so a numeric 'NA' stops "
                        "ingestion as not a number instead of becoming null."
                    ),
                ),
                SentinelRule(
                    marker="0",
                    meaning="not_applicable",
                    replacement=None,
                    evidence=(
                        "ECCC Climate Data Online Glossary, Wind Direction: tens "
                        "of degrees, 36 is north and zero denotes a calm wind. A "
                        "calm wind has no direction, so the store keeps none "
                        "rather than a bearing of 0 degrees that circular "
                        "statistics would read as north; the ECCC bulk hourly CSV "
                        "likewise leaves direction blank in calm hours. Scoped to "
                        "wind_direction only; a wind speed of 0 is a measured zero."
                    ),
                ),
            ),
            documentation=DocumentationSpec(
                source_format=(
                    "MSC GeoMet OGC API climate-hourly f=csv (pygeoapi 0.20.0): "
                    "UTF-8 CSV with CRLF line endings, the fixed 41-column header, "
                    "one row per Climate ID and LOCAL_DATE, empty field = null. One "
                    "request per Climate ID and LST calendar-year window, whose "
                    "inclusive datetime filter ends one second before the next "
                    "window (sortby=LOCAL_DATE, limit=10000, offset paging); the "
                    "window's record count is evidenced by a resulttype=hits "
                    "GeoJSON response. A tab-separated selection manifest written "
                    "by `research-store fetch` names every response with its "
                    "request URL, retrieval time and SHA-256."
                ),
                references=(
                    "Environment and Climate Change Canada, MSC GeoMet OGC API "
                    "collection climate-hourly ('Climate - Hourly Observations'), "
                    "collection metadata and queryables, "
                    "https://api.weather.gc.ca/collections/climate-hourly, "
                    "retrieved 2026-09-25 (pygeoapi 0.20.0).",
                    "Environment and Climate Change Canada, MSC Open Data: "
                    "GeoMet-OGC-API technical documentation, "
                    "https://eccc-msc.github.io/open-data/msc-geomet/ogc_api_en/, "
                    "accessed 2026-09-25: f=csv output, 10 000 features per "
                    "query, offset paging and resulttype=hits numberMatched.",
                    "Environment and Climate Change Canada, Climate Data Online "
                    "Glossary, https://climate.weather.gc.ca/glossary_e.html, "
                    "modified 2026-08-10: Local Standard Time; Total Hourly "
                    "Precipitation (element 262, minutes 00 through 60); R status "
                    "before and Q status from 2013-12-10; wind direction, calm, "
                    "wind speed, humidex, wind chill, station pressure.",
                    "Environment and Climate Change Canada, Technical "
                    "documentation: Historical Hourly Climate Station Data, "
                    "canada.ca, modified 2023-05-01: wind direction in tens of "
                    "degrees true with 0 denoting calm; wind speed at 10 m "
                    "averaged over the 1, 2 or 10 minutes ending at the "
                    "observation; humidex displayed only at 20 degC or above and "
                    "at least 1 degree above the air temperature; wind chill at "
                    "or below 0 degC.",
                    "Environment and Climate Change Canada Data Services End-use "
                    "Licence, Version 2.1.1, August 2026, "
                    "https://eccc-msc.github.io/open-data/licence/readme_en/: "
                    "attribution 'Data Source: Environment and Climate Change "
                    "Canada'; information licensed 'as is'.",
                    "Environment and Climate Change Canada, Climate Data Online "
                    "FAQ, https://climate.weather.gc.ca/FAQ_e.html: flag 'M' "
                    "denotes missing data.",
                ),
                quality_control=(
                    "Values are published as served by the live National Climate "
                    "Archive. Only basic automatic assessment at the ingest stage "
                    "(status 'Q' from 2013-12-10, 'R' raw before) is documented; "
                    "no per-record status is delivered. Source flags are kept "
                    "verbatim and nothing is filtered at ingestion. ECCC can revise "
                    "values, so each ingest is a replacement snapshot and earlier "
                    "snapshots stay readable by ID."
                ),
                quality_flags={
                    "blank": "No source flag.",
                    "M": "Missing; the value is not available and cannot be retrieved.",
                },
                limitations=(
                    "The collection serves only a subset of ECCC climate stations "
                    "(cities of 10 000+, Regional Basic Climatological Network "
                    "stations and stations with 30+ years of data); a Climate ID "
                    "it does not serve returns no records and the fetch stops.",
                    "Station figures in these limitations are for HAMILTON RBG CS "
                    "(6153301), 2000-08-17 to 2026-08-31 LST as served on "
                    "2026-09-25, compared with the ECCC bulk hourly CSVs for "
                    "2013-2025 downloaded in early 2025; "
                    "docs/ingestion-reports/2026-09-25-eccc-climate-hourly.md "
                    "derives them.",
                    "Calm wind: the publisher reports direction 0 and the store "
                    "keeps no direction (null). A null direction with a blank "
                    "flag and a non-null speed is a calm report (48,299 hours at "
                    "HAMILTON RBG CS); a missing direction carries flag 'M'. In "
                    "131 hours, all in 2018, the publisher reports calm at a "
                    "speed of 1 km/h. In 650 hours the bulk CSV has 0 km/h and no "
                    "direction where the API has 1 km/h (267) or 2 km/h (383): "
                    "105 in 2013-01 and 545 in 2018-07 to 2018-11. The API gives "
                    "a direction in 519 of them, and the other 131 are the "
                    "calm-at-1-km/h hours. Wind statistics from this dataset and "
                    "from the bulk CSVs therefore differ slightly.",
                    "Values are live and revisable; a later ingest can differ from "
                    "an earlier one for the same hour. Pin frame.attrs['snapshot_id'].",
                    "July 2022, most likely revised by ECCC after the bulk-CSV "
                    "download: between 2022-07-08 14:00 and 2022-07-20 15:00 LST "
                    "the API reports 289 precipitation hours as null and flagged "
                    "'M' where the bulk CSV and eccc_hly01_observations have "
                    "values (9 wet, 32.8 mm in total), and reports calm (0 km/h) "
                    "in 9 hours where the bulk CSV has 3 km/h and a direction.",
                    "The API serves no record for 21 hours in which the bulk CSV "
                    "has values: 2015-05-25 14:00 to 2015-05-26 09:00 and "
                    "2022-04-22 19:00 LST. They are absent from the publisher's "
                    "responses and from its numberMatched counts, so they are not "
                    "ingestion losses; eccc_hly01_observations holds their "
                    "precipitation.",
                    "In 97 hours of 2024-10 wind speed is null and flagged 'M' "
                    "but a direction is stored; the bulk CSV flags the speed 'M' "
                    "too and leaves the direction blank with a blank flag.",
                    "HAMILTON RBG CS has no records from 2017-10-30 11:00 to "
                    "2018-01-10 11:00 LST. The gap is in the publisher's archive "
                    "(the bulk CSVs are blank there too), not a fetch or "
                    "ingestion loss.",
                    "A blank flag does not prove a reported value. Precipitation "
                    "is null with a blank flag in 32,856 hours, all before 2019, "
                    "because the publisher flags missing precipitation 'M' only "
                    "from 2019: 31,953 before the first gauge value at "
                    "2004-04-19 13:00 LST, 18 from 2004-04-19 16:00 to "
                    "2004-04-20 09:00 LST, and 885 in 2007-2016. Visibility is "
                    "null with a blank flag from 2005. At 2009-03-31 20:00 LST "
                    "only precipitation (0.0 mm) is reported, and at 2016-08-22 "
                    "03:00 LST only temperature, dew point and relative "
                    "humidity; the other values of those hours, wind speed, "
                    "wind direction and station pressure included, are null "
                    "with blank flags, so a null wind direction there is not "
                    "calm. At 2020-06-10 23:00 LST air temperature is null with "
                    "a blank flag while dew point, humidity, precipitation, wind "
                    "and pressure are reported.",
                    "Coverage at HAMILTON RBG CS: temperature, dew point and "
                    "precipitation have long 'M' gaps in 2019 (4,308, 4,271 and "
                    "3,193 values in 8,753 hours) and 2020 (5,519, 5,008 and "
                    "5,559 values in 8,760 hours); station pressure starts in "
                    "2013.",
                    "A window retrieved less than api.settle_days after it ended "
                    "(the current year, typically) can still gain hours; each "
                    "window's settled_at_retrieval flag and last LOCAL_DATE are "
                    "recorded with the run as completeness_evidence.",
                    "Humidex and wind chill are absent outside their display "
                    "conditions; a null there is not missing data.",
                    "Automatic stations report no visibility or present weather.",
                    "Anemometer height and averaging window vary by station and "
                    "era and are not delivered per record.",
                    "Hourly flag codes other than blank and 'M' are not documented "
                    "by the collection; any such code is preserved and counted and "
                    "must be documented here before it is interpreted.",
                ),
                notes=(
                    "LOCAL_DATE H is the observation time and PRECIP_AMOUNT the "
                    "total for the hour ending then. At 410 rain onsets in the "
                    "2013-2025 ECCC bulk hourly CSV for HAMILTON RBG CS "
                    "(identical to this collection where compared) relative "
                    "humidity rises 10.3 points and temperature falls 1.5 degC "
                    "between the readings at H-1 and H, and barely changes from "
                    "H to H+1; eccc_hly03_observations element 123, whose slots "
                    "ECCC documents as hours ending 01-24, shows the same "
                    "signature; at its onsets the bulk-CSV total labelled with the "
                    "same ending hour is non-zero in 125 of 128 and the one "
                    "labelled an hour earlier in 4 of 126 (checked 2026-09-25).",
                    "precipitation_amount_1h is ECCC element 262 and equals "
                    "eccc_hly01_observations.precipitation_amount_1h value for "
                    "value when LOCAL_DATE H is matched to HLY01 source slot H "
                    "(HAMILTON RBG CS: 2,267 of 2,267 hours in 2013-04, 2018-07, "
                    "2020-05 and 2021-01, -03 and -11, checked 2026-09-25; the "
                    "ingestion report repeats it for the full record). HLY01 "
                    "keys that slot one hour later, [H, H+1), which its own "
                    "documentation records as a defect pending a controlled "
                    "replacement, so until then this dataset's time_start equals "
                    "HLY01's time_start minus one hour for the same total. "
                    "Neither dataset is declared authoritative; each analysis "
                    "chooses.",
                    "LOCAL_DATE is converted to UTC through eccc_station_inventory "
                    "and the evidence-backed Climate ID overrides shared with the "
                    "HLY archive; the publisher's own UTC_DATE is cross-checked "
                    "against the converted observation time (time_end) and a "
                    "disagreeing record is quarantined.",
                    "WIND_DIRECTION is multiplied by 10 to degrees true and a calm "
                    "0 is stored as null. Every other raw direction must be an "
                    "integer from 1 to 36; any other value stops ingestion, "
                    "because it means the publisher's encoding changed.",
                    "Not republished, because they are redundant or held in the "
                    "station inventory: x, y, ID, STATION_NAME, PROVINCE_CODE, "
                    "LATITUDE/LONGITUDE_DECIMAL_DEGREES, the LOCAL_* and UTC_* "
                    "components, and WEATHER_FRE_DESC. ID and the LOCAL_* and "
                    "UTC_* components must agree with LOCAL_DATE and UTC_DATE; x, "
                    "y, STATION_NAME, PROVINCE_CODE, STN_ID and the decimal-degree "
                    "coordinates must be constant over each response, with x and "
                    "y equal to the coordinates; otherwise ingestion stops. The "
                    "constant values are recorded per response in "
                    "ingestion_inputs (page_constants). WEATHER_FRE_DESC is not "
                    "checked. All of them remain in the archived raw bytes.",
                    "Attribution required by the licence: 'Data Source: "
                    "Environment and Climate Change Canada'.",
                ),
            ),
            ingest_options={
                "api": {
                    "collection_url": (
                        "https://api.weather.gc.ca/collections/climate-hourly"
                    ),
                    "items_url": (
                        "https://api.weather.gc.ca/collections/climate-hourly/items"
                    ),
                    "format": "csv",
                    "limit": 10000,
                    "sortby": "LOCAL_DATE",
                    "window": "local_calendar_year",
                    # On 2026-09-25 the collection held HAMILTON RBG CS to
                    # 2026-09-23 23:00, and ECCC can still fill gaps after
                    # that. A response is final only once its window ended this
                    # long before it was retrieved; until then `fetch` asks
                    # again rather than reusing its cache.
                    "settle_days": 7,
                },
                "encoding": "utf-8",
                "source_columns": list(_GEOMET_CLIMATE_HOURLY_COLUMNS),
                "entity_column": "CLIMATE_IDENTIFIER",
                "local_time_column": "LOCAL_DATE",
                "local_time_format": "%Y-%m-%d %H:%M:%S",
                # LOCAL_DATE is the observation time, the END of the hour whose
                # total PRECIP_AMOUNT reports; see the documentation notes.
                "local_time_labels": "slot_end",
                "local_component_columns": [
                    "LOCAL_YEAR",
                    "LOCAL_MONTH",
                    "LOCAL_DAY",
                    "LOCAL_HOUR",
                ],
                "publisher_utc_column": "UTC_DATE",
                "publisher_utc_format": "%Y-%m-%dT%H:%M:%S",
                "utc_component_columns": ["UTC_YEAR", "UTC_MONTH", "UTC_DAY"],
                "record_id_column": "ID",
                "record_id_format": "{entity}.{year}.{month}.{day}.{hour}",
                # Station metadata repeated on every row: one value per response.
                "page_constant_columns": [
                    "x",
                    "y",
                    "STATION_NAME",
                    "PROVINCE_CODE",
                    "STN_ID",
                    "LONGITUDE_DECIMAL_DEGREES",
                    "LATITUDE_DECIMAL_DEGREES",
                ],
                "coordinate_columns": {
                    "x": "LONGITUDE_DECIMAL_DEGREES",
                    "y": "LATITUDE_DECIMAL_DEGREES",
                },
                "column_map": {
                    "air_temperature": "TEMP",
                    "air_temperature_quality": "TEMP_FLAG",
                    "dew_point_temperature": "DEW_POINT_TEMP",
                    "dew_point_temperature_quality": "DEW_POINT_TEMP_FLAG",
                    "relative_humidity": "RELATIVE_HUMIDITY",
                    "relative_humidity_quality": "RELATIVE_HUMIDITY_FLAG",
                    "precipitation_amount_1h": "PRECIP_AMOUNT",
                    "precipitation_amount_1h_quality": "PRECIP_AMOUNT_FLAG",
                    "wind_direction": "WIND_DIRECTION",
                    "wind_direction_quality": "WIND_DIRECTION_FLAG",
                    "wind_speed": "WIND_SPEED",
                    "wind_speed_quality": "WIND_SPEED_FLAG",
                    "visibility": "VISIBILITY",
                    "visibility_quality": "VISIBILITY_FLAG",
                    "station_pressure": "STATION_PRESSURE",
                    "station_pressure_quality": "STATION_PRESSURE_FLAG",
                    "humidex": "HUMIDEX",
                    "humidex_quality": "HUMIDEX_FLAG",
                    "wind_chill": "WINDCHILL",
                    "wind_chill_quality": "WINDCHILL_FLAG",
                    "weather_description": "WEATHER_ENG_DESC",
                },
                "annotation_map": {
                    "source_station_id": "STN_ID",
                    "record_flag": "FLAG",
                },
                # Every numeric variable declares its publisher scale; the parser
                # refuses one that is missing. Wind direction is delivered in tens
                # of degrees.
                "scales": {
                    "air_temperature": 1.0,
                    "dew_point_temperature": 1.0,
                    "relative_humidity": 1.0,
                    "precipitation_amount_1h": 1.0,
                    "wind_direction": 10.0,
                    "wind_speed": 1.0,
                    "visibility": 1.0,
                    "station_pressure": 1.0,
                    "humidex": 1.0,
                    "wind_chill": 1.0,
                },
                # The publisher's documented raw encoding, checked value by
                # value before scaling: tens of degrees, 0 (calm) to 36 (north).
                "source_integer_ranges": {"wind_direction": [0, 36]},
                "sentinel_variables": {
                    "": [
                        "air_temperature",
                        "dew_point_temperature",
                        "relative_humidity",
                        "precipitation_amount_1h",
                        "wind_direction",
                        "wind_speed",
                        "visibility",
                        "station_pressure",
                        "humidex",
                        "wind_chill",
                        "weather_description",
                    ],
                    "NA": ["weather_description"],
                    "0": ["wind_direction"],
                },
                # Where each variable sits in the row's one-hour slot
                # [H-1h, H) LST: minute 60 is H, the observation time.
                "variable_timing": {
                    "air_temperature": {"end_minute": 60, "duration_minutes": 0},
                    "dew_point_temperature": {"end_minute": 60, "duration_minutes": 0},
                    "relative_humidity": {"end_minute": 60, "duration_minutes": 0},
                    "precipitation_amount_1h": {
                        "start_minute": 0,
                        "duration_minutes": 60,
                    },
                    "wind_direction": {"end_minute": 60, "duration_minutes": 2},
                    "wind_speed": {"end_minute": 60, "duration_minutes": [1, 2, 10]},
                    "visibility": {"end_minute": 60, "duration_minutes": 0},
                    "station_pressure": {"end_minute": 60, "duration_minutes": 0},
                    "humidex": {"end_minute": 60, "duration_minutes": 0},
                    "wind_chill": {"end_minute": 60, "duration_minutes": 0},
                    "weather_description": {"end_minute": 60, "duration_minutes": 0},
                },
                "timezone_policy": "station_inventory",
                "station_dataset_id": "eccc_station_inventory",
                "timezone_overrides": dict(_ECCC_HOURLY_TIMEZONE_OVERRIDES),
                "missing_timezone_policy": "quarantine_record",
                "standard_time_transition_policy": "quarantine_record",
                "publisher_utc_mismatch_policy": "quarantine_record",
                "manifest_delimiter": "\t",
                "manifest_columns": [
                    "climate_id",
                    "window_start_lst",
                    "window_end_lst",
                    "role",
                    "page_offset",
                    "filename",
                    "size_bytes",
                    "sha256",
                    "request_url",
                    "retrieved_at",
                    "http_date",
                    "content_type",
                    "server",
                    "number_matched",
                ],
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
