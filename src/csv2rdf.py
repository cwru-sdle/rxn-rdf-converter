import pandas as pd
import os
import re
import ord_schema
from ord_schema.message_helpers import load_message, write_message, message_to_row
from ord_schema.proto import dataset_pb2, reaction_pb2
from rxn_rdf_converter import ReactionKG

dataset_path = '/mnt/vstor/CSE_MSE_RXF131/staging/mds3/KG-ChemRxn/OpenReactionDB/ord-data/data'
owl_onto = '/home/qdt/CSE_MSE_RXF131/staging/mds3/KG-ChemRxn/chemOntologies/mdsChemRxn(v.0.3.0.7).owl'
#'/mnt/vstor/CSE_MSE_RXF131/cradle-members/mds3/qdt/git/cwrusdle.bitbucket.io/files/MDS_Onto-v0.3.1.14.owl'
output_dir = '/mnt/vstor/CSE_MSE_RXF131/staging/mds3/KG-ChemRxn'
file_list = []
for root, dirs, files in os.walk(dataset_path):
    for name in files: 
        # ORD datasets typically start with 'ord_dataset' and are .pb files
        if name.startswith('ord_dataset') and name.endswith('.pb'):
            file_path = os.path.join(root, name)
            file_list.append(file_path)
dataset = load_message(file_list[1], dataset_pb2.Dataset)
dataset_id = dataset.dataset_id
reaction = dataset.reactions[1]
reaction_data = ReactionKG(reaction, 'turtle').generate_reaction().generate_instances(owl_onto).generate_data_graph(dataset.dataset_id, output_dir)
                
identifier_df = pd.DataFrame(columns=['reactionID', 'type', 'value', 'is_mapped'])
setup_df = pd.DataFrame(columns=['reactionID', 'setup.environment'])
compound_df = pd.DataFrame(columns=['reactionID', 'INCHI_KEY', 'reaction_role' 'identifier.type', 'identifier.value','amount.type', 'amount.value', 'amount.unit'])
input_df = pd.DataFrame(columns=['reactionID', 'addition_order', 
        'addition_time.value', 'addition_time.units', 
        'addition_speed.type', 
        'addition_duration.value', 'addition_duration.units', 
        'addition_device.type', 'addition_temperature', 
        'flow_rate.value', 'flow_rate.units', 'texture'])

condition_df = pd.DataFrame(columns=['reactionID', 'temperature.setpoint.value', 'temperature.setpoint.units', 'temperature.control', 
        'pressure.setpoint.value', 'pressure.setpoint.units', 'pressure.atmosphere', 'pressure.control',
        'stirring.type', 'stirring.rate',
        'illumination.type', 'illumination.color', 'illumination.peak_wavelength', 'illumination.distance_to_vessel',
        ])
notes_df = pd.DataFrame(columns=['reactionID', 'is_heterogenous', 
        'forms_precipitate', 'is_exothermic', 'off_gases', 
        'is_sensitive_to_moisture', 'is_sensitive_to_light', 'is_sensitive_to_oxygen', 
        'safety_notes', 'procedure_details'
])
workup_df = pd.DataFrame(columns=['reactionID', 'workup.type', 'workup.temperature', 'workup.duration', 'workup.details'])

outcome_df = pd.DataFrame(columns=['reactionID'])

