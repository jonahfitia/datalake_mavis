import json
import pandas as pd
from datetime import datetime

def load_fhir_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def create_pivot_schema(fhir_bundle, table_to_include=None):
    patient_data = []
    diagnostic_data = []
    encounter_data = []
    medication_data = []

    if not table_to_include:
        table_to_include = ["Patient", "DiagnosticReport", "Encounter", "MedicationRequest"]
        print(f"Aucune table spécifiée dans table_to_include. Traitement de toutes les ressources : {table_to_include}")

    for entry in fhir_bundle.get('entry', []):
        resource = entry.get('resource', {})
        resource_type = resource.get('resourceType')

        if resource_type not in table_to_include:
            continue

        if resource_type == 'Patient':
            patient = {
                'patient_id': resource.get('id'),
                'marital_status': resource.get('maritalStatus', {}).get('coding', [{}])[0].get('code'),
                'birth_weight': next((ext.get('valueQuantity', {}).get('value') for ext in resource.get('extension', []) if ext.get('url', '').endswith('birthWeight')), None),
                'currently_pregnant': next((ext.get('valueBoolean') for ext in resource.get('extension', []) if ext.get('url', '').endswith('currentlyPregnant')), None),
                'last_updated': resource.get('meta', {}).get('lastUpdated')
            }
            patient_data.append(patient)

        elif resource_type == 'DiagnosticReport':
            diagnostic = {
                'report_id': resource.get('id'),
                'patient_id': resource.get('subject', {}).get('reference', '').replace('Patient/', ''),
                'test_type': resource.get('code', {}).get('coding', [{}])[0].get('display'),
                'total_price': next((ext.get('valueMoney', {}).get('value') for ext in resource.get('extension', []) if ext.get('url', '').endswith('total-price')), None),
                'issued': resource.get('issued'),
                'last_updated': resource.get('meta', {}).get('lastUpdated')
            }
            diagnostic_data.append(diagnostic)

        elif resource_type == 'Encounter':
            encounter = {
                'encounter_id': resource.get('id'),
                'patient_id': resource.get('subject', {}).get('reference', '').replace('Patient/', ''),
                'type': resource.get('class', {}).get('display'),
                'start_date': resource.get('period', {}).get('start'),
                'end_date': resource.get('period', {}).get('end'),
                'reason': resource.get('reasonCode', [{}])[0].get('text') or '',
                'diagnosis_code': next((diag.get('condition', {}).get('coding', [{}])[0].get('code') for diag in resource.get('diagnosis', [])), None),
                'last_updated': resource.get('meta', {}).get('lastUpdated')
            }
            encounter_data.append(encounter)

        elif resource_type == 'MedicationRequest':
            medication = {
                'medication_id': resource.get('id'),
                'patient_id': resource.get('subject', {}).get('reference', '').replace('Patient/', ''),
                'authored_on': resource.get('authoredOn'),
                'notes': resource.get('note', [{}])[0].get('text') or '',
                'last_updated': resource.get('meta', {}).get('lastUpdated')
            }
            medication_data.append(medication)

    df_patient = pd.DataFrame(patient_data)
    df_diagnostic = pd.DataFrame(diagnostic_data)
    df_encounter = pd.DataFrame(encounter_data)
    df_medication = pd.DataFrame(medication_data)

    # Harmoniser les types de patient_id avant les merges
    df_patient['patient_id'] = df_patient['patient_id'].astype(str)
    if not df_diagnostic.empty:
        df_diagnostic['patient_id'] = df_diagnostic['patient_id'].astype(str)
    if not df_encounter.empty:
        df_encounter['patient_id'] = df_encounter['patient_id'].astype(str)
    if not df_medication.empty:
        df_medication['patient_id'] = df_medication['patient_id'].astype(str)
        
    df_pivot = df_patient
    if not df_diagnostic.empty:
        df_pivot = df_pivot.merge(df_diagnostic, on='patient_id', how='left', suffixes=('', '_diagnostic'))
    if not df_encounter.empty:
        df_pivot = df_pivot.merge(df_encounter, on='patient_id', how='left', suffixes=('', '_encounter'))
    if not df_medication.empty:
        df_pivot = df_pivot.merge(df_medication, on='patient_id', how='left', suffixes=('', '_medication'))

    return df_pivot

def main():
    file_path = 'fhir.json'
    fhir_bundle = load_fhir_json(file_path)
    table_to_include = ["Patient", "DiagnosticReport", "Encounter", "MedicationRequest"]
    df_pivot = create_pivot_schema(fhir_bundle, table_to_include)
    df_pivot.to_csv('/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/pivot_schema.csv', index=False)
    print("Schéma pivot généré et sauvegardé dans 'pivot_schema.csv'")

if __name__ == "__main__":
    main()