from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


FHIR_DIR = Path("data/fhir")


def die(msg: str) -> None:
    raise RuntimeError(f"FHIR INGEST FAILED: {msg}")


def load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"Could not parse JSON: {path} ({e})")


def ref_id(ref: Optional[str]) -> Optional[str]:
    """
    Convert 'Patient/patient-001' -> 'patient-001'
    If ref is already an id-like string, return as-is.
    """
    if not ref:
        return None
    if "/" in ref:
        return ref.split("/")[-1]
    return ref


def coding_pick(resource: Dict[str, Any], coding_path: Tuple[str, ...]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Convenience: pull a (system, code, display) from nested coding arrays.
    Example paths:
      ("code", "coding") for Observation.code.coding
      ("class",) for Encounter.class (not an array, handled separately below)
    """
    cur: Any = resource
    for k in coding_path:
        if not isinstance(cur, dict) or k not in cur:
            return (None, None, None)
        cur = cur[k]

    # cur should be a list of codings
    if isinstance(cur, list) and len(cur) > 0 and isinstance(cur[0], dict):
        c0 = cur[0]
        return (c0.get("system"), c0.get("code"), c0.get("display"))
    return (None, None, None)


def ingest_bundle(bundle: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if bundle.get("resourceType") != "Bundle":
        die("JSON was not a FHIR Bundle (resourceType != 'Bundle')")

    entries = bundle.get("entry", [])
    if not isinstance(entries, list) or len(entries) == 0:
        die("Bundle.entry is empty or invalid")

    patients: List[Dict[str, Any]] = []
    encounters: List[Dict[str, Any]] = []
    observations: List[Dict[str, Any]] = []
    chargeitems: List[Dict[str, Any]] = []

    for ent in entries:
        res = (ent or {}).get("resource")
        if not isinstance(res, dict):
            continue

        rtype = res.get("resourceType")
        rid = res.get("id")

        if rtype == "Patient":
            ident = None
            identifiers = res.get("identifier") or []
            if isinstance(identifiers, list) and identifiers:
                ident = identifiers[0].get("value")

            name = None
            names = res.get("name") or []
            if isinstance(names, list) and names:
                n0 = names[0]
                given = " ".join(n0.get("given", []) or [])
                family = n0.get("family")
                name = " ".join([x for x in [given, family] if x])

            patients.append(
                {
                    "patient_id": rid,
                    "mrn": ident,
                    "name": name,
                    "gender": res.get("gender"),
                    "birth_date": res.get("birthDate"),
                }
            )

        elif rtype == "Encounter":
            subj = ref_id((res.get("subject") or {}).get("reference"))
            period = res.get("period") or {}
            start = period.get("start")
            end = period.get("end")

            # Encounter.class is a single coding object (not array)
            cls = res.get("class") or {}
            class_system = cls.get("system")
            class_code = cls.get("code")
            class_display = cls.get("display")

            # department-ish: we’ll store Encounter.location[0].location.display as department_name-ish
            dept = None
            loc = res.get("location") or []
            if isinstance(loc, list) and loc:
                l0 = loc[0].get("location") or {}
                dept = l0.get("display")

            # provider-ish: participant[0].individual.display
            provider = None
            part = res.get("participant") or []
            if isinstance(part, list) and part:
                p0 = part[0].get("individual") or {}
                provider = p0.get("display")

            encounters.append(
                {
                    "encounter_id": rid,
                    "patient_id": subj,
                    "status": res.get("status"),
                    "class_system": class_system,
                    "class_code": class_code,
                    "class_display": class_display,
                    "start_ts": start,
                    "end_ts": end,
                    "department": dept,
                    "provider_name": provider,
                }
            )

        elif rtype == "Observation":
            subj = ref_id((res.get("subject") or {}).get("reference"))
            enc = ref_id((res.get("encounter") or {}).get("reference"))
 
            (code_system, code_code, code_display) = coding_pick(res, ("code", "coding"))

            eff = res.get("effectiveDateTime")
            valq = res.get("valueQuantity") or {}
            observations.append(
                {
                    "observation_id": rid,
                    "patient_id": subj, 
                    "encounter_id": enc,
                    "loinc_system": code_system,
                    "loinc_code": code_code,
                    "loinc_display": code_display or (res.get("code") or {}).get("text"),
                    "effective_ts": eff,
                    "value": valq.get("value"),
                    "unit": valq.get("unit"),
                }
            )

        elif rtype == "ChargeItem": 
            subj = ref_id((res.get("subject") or {}).get("reference"))
            enc = ref_id((res.get("context") or {}).get("reference"))

            (cpt_system, cpt_code, cpt_display) = coding_pick(res, ("code", "coding"))

            occ = res.get("occurrenceDateTime")
            price = (res.get("priceOverride") or {}).get("value")
            currency = (res.get("priceOverride") or {}).get("currency") 
            qty = (res.get("quantity") or {}).get("value")

            chargeitems.append(
                {
                    "chargeitem_id": rid,
                    "patient_id": subj,
                    "encounter_id": enc,
                    "cpt_system": cpt_system, 
                    "cpt_code": cpt_code,
                    "cpt_display": cpt_display or (res.get("code") or {}).get("text"),
                    "occurrence_ts": occ,
                    "quantity": qty,
                    "amount": price,
                    "currency": currency,
                }
            )

        # ignore other resources for now (Practitioner, Organization, etc.)

    df_pat = pd.DataFrame(patients)
    df_enc = pd.DataFrame(encounters)
    df_obs = pd.DataFrame(observations)
    df_chg = pd.DataFrame(chargeitems) 

    return df_pat, df_enc, df_obs, df_chg
 

def main() -> None:
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        die("DATABASE_URL not set (.env missing?)")

    engine = create_engine(db_url, future=True)

    if not FHIR_DIR.exists():
        die(f"Missing folder: {FHIR_DIR}. Create it and add sample_bundle.json") 

    files = sorted(FHIR_DIR.glob("*.json"))
    if not files:
        die(f"No .json files found in {FHIR_DIR}")

    # Combine all bundles into one staging load
    all_pat, all_enc, all_obs, all_chg = [], [], [], [] 
    for fp in files:
        bundle = load_json(fp)
        df_pat, df_enc, df_obs, df_chg = ingest_bundle(bundle)
        df_pat["source_file"] = fp.name
        df_enc["source_file"] = fp.name 
        df_obs["source_file"] = fp.name
        df_chg["source_file"] = fp.name

        all_pat.append(df_pat) 
        all_enc.append(df_enc)
        all_obs.append(df_obs)
        all_chg.append(df_chg)

    stg_pat = pd.concat(all_pat, ignore_index=True) if all_pat else pd.DataFrame()
    stg_enc = pd.concat(all_enc, ignore_index=True) if all_enc else pd.DataFrame()
    stg_obs = pd.concat(all_obs, ignore_index=True) if all_obs else pd.DataFrame()
    stg_chg = pd.concat(all_chg, ignore_index=True) if all_chg else pd.DataFrame()

    # Basic guardrails (helpful in interviews)
    if stg_pat.empty:
        die("No Patient resources found")
    if stg_enc.empty:
        die("No Encounter resources found")
    # Observation/ChargeItem can be empty in some feeds; we allow it. 

    # Write staging tables
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS etl_run_log (run_id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ, status TEXT NOT NULL DEFAULT 'running', notes TEXT);"))
        run_id = conn.execute(text("INSERT INTO etl_run_log(status, notes) VALUES ('running', 'ingest fhir bundle(s)') RETURNING run_id;")).scalar_one()

    stg_pat.to_sql("stg_fhir_patient", engine, if_exists="replace", index=False, method="multi", chunksize=5000)
    stg_enc.to_sql("stg_fhir_encounter", engine, if_exists="replace", index=False, method="multi", chunksize=5000)
    stg_obs.to_sql("stg_fhir_observation", engine, if_exists="replace", index=False, method="multi", chunksize=5000) 
    stg_chg.to_sql("stg_fhir_chargeitem", engine, if_exists="replace", index=False, method="multi", chunksize=5000)

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE etl_run_log SET finished_at = now(), status = 'success', notes = :notes WHERE run_id = :run_id"),
            {
                "run_id": run_id,
                "notes": f"FHIR staged: patient={len(stg_pat)}, encounter={len(stg_enc)}, obs={len(stg_obs)}, chargeitem={len(stg_chg)}",
            },
        )

    print("FHIR staging load complete:")
    print(f" - stg_fhir_patient:     {len(stg_pat):,} rows")
    print(f" - stg_fhir_encounter:   {len(stg_enc):,} rows")
    print(f" - stg_fhir_observation: {len(stg_obs):,} rows")
    print(f" - stg_fhir_chargeitem:  {len(stg_chg):,} rows")


if __name__ == "__main__":
    main()


# Cool FHIR synthetic data production project by two healthcare analysts - maybe incorporate later
# https://github.com/smart-on-fhir/sample-bulk-fhir-datasets.git