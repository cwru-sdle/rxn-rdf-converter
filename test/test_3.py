import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from rdflib import Graph, URIRef, Literal

# Adjust imports based on your package structure
from rxn_rdf_converter.dataset_processor import DatasetProcessor
from rxn_rdf_converter.reactionkg import ReactionKG, SpaceInsensitiveDict


# =====================================================================
#               MOCK HELPERS FOR OWLREADY2 & PROTOBUFS
# =====================================================================

class MockOwlEntity:
    """Simulates an owlready2 Class or Individual."""
    def __init__(self, name, base_iri="https://cwrusdle.bitbucket.io/files/MDS_Onto/index-en.html#", label=None):
        self.name = name
        self.iri = f"{base_iri}{name}"
        self.label = [label or name]
        self.is_instance_of = [self]

    def __call__(self, instance_id):
        inst = MockOwlEntity(instance_id, base_iri=self.iri.rsplit('#', 1)[0] + '#')
        inst.is_instance_of = [self]
        return inst

    def __str__(self):
        return self.name


class MockNamespace:
    """Simulates an owlready2 Namespace that dynamically generates MockOwlEntity classes."""
    def __init__(self, base_iri):
        self.base_iri = base_iri

    def __getattr__(self, name):
        return MockOwlEntity(name, base_iri=self.base_iri, label=name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def build_mock_ontology():
    """Creates a mock owlready2 Ontology with standard properties used in ReactionKG."""
    mock_onto = MagicMock()

    def get_ns(base_iri):
        return MockNamespace(base_iri)

    mock_onto.get_namespace.side_effect = get_ns

    object_props = [
        'has output', 'is input of', 'environs', 'designates', 'member part of',
        'inheres in', 'uses measurement unit', 'has process part', 'has occurent part',
        'occupies temporal region', 'participates in', 'is about', 'affects',
        'bearer of', 'is made of', 'precedes'
    ]
    data_props = [
        'has text value', 'details', 'is mapped', 'is limiting', 'is desired product',
        'isolated color', 'has decimal value', 'addition order', 'has datetime value',
        'is heterogeneous', 'forms Precipitate', 'is exothermic', 'off gasses',
        'is sensitive to moisture', 'is sensitive to oxygen', 'is sensitive to light',
        'safety notes', 'procedure details', 'keep phase', 'is automated',
        'uses internal standard', 'uses authentic standard', 'is normalized',
        'is of isolated species', 'has integer value'
    ]

    obj_prop_mocks = [
        MockOwlEntity(p.replace(" ", "_"), label=p) for p in object_props
    ]
    data_prop_mocks = [
        MockOwlEntity(p.replace(" ", "_"), label=p) for p in data_props
    ]

    mock_onto.object_properties.return_value = obj_prop_mocks
    mock_onto.annotation_properties.return_value = []
    mock_onto.data_properties.return_value = data_prop_mocks
    mock_onto.properties.return_value = obj_prop_mocks + data_prop_mocks

    return mock_onto


def create_mock_pb_field_container(fields_dict):
    """Creates a mock Protobuf message with DESCRIPTOR.fields_by_name and attributes."""
    pb = MagicMock()
    pb.DESCRIPTOR.fields_by_name = {k: MagicMock() for k in fields_dict.keys()}
    for k, v in fields_dict.items():
        setattr(pb, k, v)
    return pb


# =====================================================================
#               1. TEST SPACE INSENSITIVE DICT
# =====================================================================

class TestSpaceInsensitiveDict(unittest.TestCase):
    """Tests for the SpaceInsensitiveDict utility class."""

    def setUp(self):
        self.d = SpaceInsensitiveDict()

    def test_set_and_get_with_spaces(self):
        self.d["has text value"] = ["val1"]
        self.assertEqual(self.d["hastextvalue"], ["val1"])
        self.assertEqual(self.d["has text value"], ["val1"])
        self.assertEqual(self.d["  has  text  value  "], ["val1"])

    def test_contains_and_get_method(self):
        self.d["is input of"] = "node_1"
        self.assertIn("isinputof", self.d)
        self.assertIn("is input of", self.d)
        self.assertEqual(self.d.get("is input of"), "node_1")
        self.assertIsNone(self.d.get("non existent key"))
        self.assertEqual(self.d.get("non existent key", "default"), "default")

    def test_non_string_keys(self):
        self.d[123] = "numeric_key"
        self.assertEqual(self.d[123], "numeric_key")
        self.assertIn(123, self.d)


# =====================================================================
#               2. TEST DATASET PROCESSOR
# =====================================================================

class TestDatasetProcessor(unittest.TestCase):
    """Unit tests for DatasetProcessor in dataset_processor.py."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, "output")
        self.log_dir = os.path.join(self.temp_dir, "logs")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

        self.dataset_subdir = os.path.join(self.temp_dir, "data_folder")
        os.makedirs(self.dataset_subdir, exist_ok=True)
        self.dataset_file_path = os.path.join(self.dataset_subdir, "ord_dataset-abc12345.pb.gz")
        self.owl_path = os.path.join(self.temp_dir, "mds_onto.owl")

        # Mock dataset_pb module and loaded dataset object
        self.mock_dataset_pb_module = MagicMock()
        self.mock_loaded_dataset = MagicMock()
        self.mock_loaded_dataset.dataset_id = "ord_dataset-abc12345"

        rxn1 = MagicMock()
        rxn1.reaction_id = "ord-rxn111"
        rxn2 = MagicMock()
        rxn2.reaction_id = "ord-rxn222"
        self.mock_loaded_dataset.reactions = [rxn1, rxn2]

    def tearDown(self):
        # Close handlers to release file locks before removing temp directory
        import logging
        logger = logging.getLogger("dataset_abc12345")
        for h in list(logger.handlers):
            h.close()
            logger.removeHandler(h)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("rxn_rdf_converter.dataset_processor.load_message")
    def test_initialization_and_logger_creation(self, mock_load_message):
        mock_load_message.return_value = self.mock_loaded_dataset

        processor = DatasetProcessor(
            dataset_pb=self.mock_dataset_pb_module,
            dataset_file_path=self.dataset_file_path,
            owl_onto_file_path=self.owl_path,
            output_directory=self.output_dir,
            error_log_directory=self.log_dir,
            fmt="turtle"
        )

        self.assertEqual(processor.dataset_id, "abc12345")
        self.assertEqual(processor.fmt, "turtle")
        expected_out = os.path.join(self.output_dir, "data_folder")
        self.assertEqual(processor.output_dir, expected_out)
        self.assertTrue(os.path.isdir(expected_out))

        expected_log = os.path.join(self.log_dir, "dataset_abc12345.log")
        self.assertTrue(os.path.isfile(expected_log))

    @patch("rxn_rdf_converter.dataset_processor.ReactionKG")
    @patch("rxn_rdf_converter.dataset_processor.load_message")
    def test_extract_reaction_all_success(self, mock_load_message, mock_reaction_kg_cls):
        mock_load_message.return_value = self.mock_loaded_dataset

        # Chain calls: ReactionKG().generate_reaction().generate_instances().generate_data_graph()
        mock_kg_inst = MagicMock()
        mock_kg_inst.generate_reaction.return_value = mock_kg_inst
        mock_kg_inst.generate_instances.return_value = mock_kg_inst
        mock_kg_inst.generate_data_graph.return_value = mock_kg_inst
        mock_reaction_kg_cls.return_value = mock_kg_inst

        processor = DatasetProcessor(
            self.mock_dataset_pb_module,
            self.dataset_file_path,
            self.owl_path,
            self.output_dir,
            self.log_dir
        )

        proc_ret, errors, rxn_list = processor.extract_reaction()

        self.assertIs(proc_ret, processor)
        self.assertEqual(len(errors), 0)
        self.assertEqual(rxn_list, [
            ["abc12345", "ord-rxn111"],
            ["abc12345", "ord-rxn222"]
        ])
        self.assertEqual(mock_reaction_kg_cls.call_count, 2)

    @patch("rxn_rdf_converter.dataset_processor.ReactionKG")
    @patch("rxn_rdf_converter.dataset_processor.load_message")
    def test_extract_reaction_handles_exception_and_continues(self, mock_load_message, mock_reaction_kg_cls):
        mock_load_message.return_value = self.mock_loaded_dataset

        # First reaction raises exception, second succeeds
        mock_kg_fail = MagicMock()
        mock_kg_fail.generate_reaction.side_effect = RuntimeError("Corrupt reaction PB")

        mock_kg_ok = MagicMock()
        mock_kg_ok.generate_reaction.return_value = mock_kg_ok
        mock_kg_ok.generate_instances.return_value = mock_kg_ok
        mock_kg_ok.generate_data_graph.return_value = mock_kg_ok

        mock_reaction_kg_cls.side_effect = [mock_kg_fail, mock_kg_ok]

        processor = DatasetProcessor(
            self.mock_dataset_pb_module,
            self.dataset_file_path,
            self.owl_path,
            self.output_dir,
            self.log_dir
        )

        existing_list = [["prev_ds", "ord-rxn000"]]
        _, errors, rxn_list = processor.extract_reaction(dataset_reaction_list=existing_list)

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], "ord-rxn111")
        self.assertIn("Corrupt reaction PB", errors[0][1])
        self.assertEqual(rxn_list, [
            ["prev_ds", "ord-rxn000"],
            ["abc12345", "ord-rxn222"]
        ])


# =====================================================================
#               3. TEST REACTION KG
# =====================================================================

class TestReactionKG(unittest.TestCase):
    """Unit tests for ReactionKG in reactionkg.py."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mock_rxn_pb = MagicMock()
        self.mock_rxn_pb.reaction_id = "ord-999888777"

        # Default empty protobuf containers
        self.mock_rxn_pb.identifiers = []
        self.mock_rxn_pb.inputs = {}
        self.mock_rxn_pb.setup = create_mock_pb_field_container({'vessel': None, 'environment': None})
        self.mock_rxn_pb.conditions = create_mock_pb_field_container({
            'temperature': None, 'pressure': None, 'stirring': None,
            'illumination': None, 'electrochemistry': None, 'flow': None
        })
        self.mock_rxn_pb.notes = create_mock_pb_field_container({'is_exothermic': None, 'safety_notes': None})
        self.mock_rxn_pb.workups = []
        self.mock_rxn_pb.outcomes = []
        self.mock_rxn_pb.provenance = create_mock_pb_field_container({'experimenter': None})

        self.kg = ReactionKG(self.mock_rxn_pb, fmt="turtle")

        # Patch get_ontology to initialize ontology environment cleanly
        with patch("rxn_rdf_converter.reactionkg.get_ontology") as mock_get_onto:
            mock_onto_loader = MagicMock()
            mock_onto_loader.load.return_value = build_mock_ontology()
            mock_get_onto.return_value = mock_onto_loader
            self.kg._initialize_instance_dict("dummy_onto.owl")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # -----------------------------------------------------------------
    # Core Initialization & Helper Method Tests
    # -----------------------------------------------------------------

    def test_init_and_core_instances(self):
        self.assertEqual(self.kg.reaction_id, "999888777")
        self.assertEqual(self.kg.fmt, "turtle")
        self.assertIsNotNone(self.kg.crude_product)
        self.assertIsNotNone(self.kg.chemical_reaction)
        self.assertIsNotNone(self.kg.reaction_mixture)
        self.assertIsNotNone(self.kg.reaction_environment)
        self.assertEqual(len(self.kg.instance_dict['type']), 4)

    def test_extract_index_set_numeric_and_string(self):
        sample_dict = {
            'components[2].amount': 1,
            'components[0].amount': 2,
            'components[1].amount': 3,
            'analyses["hplc_1"].type': 'LC',
            'analyses["nmr_1"].type': 'NMR_1H',
        }
        num_indices = self.kg._extract_index_set(sample_dict, r'^components\[(\d+)\]')
        self.assertEqual(num_indices, [0, 1, 2])

        str_indices = self.kg._extract_index_set(sample_dict, r'^analyses\["([^"]*)"\]')
        self.assertEqual(set(str_indices), {"hplc_1", "nmr_1"})

        empty_indices = self.kg._extract_index_set(sample_dict, r'^nonexistent\[(\d+)\]')
        self.assertEqual(empty_indices, [])

    def test_strip_key_spaces(self):
        self.kg._strip_key_spaces()
        for k in self.kg.instance_dict.keys():
            self.assertNotIn(" ", k)
        for k in self.kg.prop_metadata_dict.keys():
            self.assertNotIn(" ", k)

    # -----------------------------------------------------------------
    # RDKit Compound Identifier Generation Tests
    # -----------------------------------------------------------------

    @patch("rxn_rdf_converter.reactionkg.message_to_row")
    def test_generate_compound_identifiers_from_smiles(self, mock_msg_to_row):
        id_pb = create_mock_pb_field_container({'type': 'SMILES', 'value': 'CCO', 'details': None})
        mock_msg_to_row.return_value = {'type': 'SMILES', 'value': 'CCO'}

        id_list, inchi_key = self.kg.generate_compound_identifiers([id_pb])

        # Ethanol InChIKey is LFQSCWFLJHTTHZ-UHFFFAOYSA-N
        self.assertEqual(inchi_key, "LFQSCWFLJHTTHZ-UHFFFAOYSA-N")
        types_present = {item['type'] for item in id_list}
        self.assertIn("SMILES", types_present)
        self.assertIn("INCHI", types_present)
        self.assertIn("INCHI_KEY", types_present)
        self.assertIn("CXSMILES", types_present)
        for item in id_list:
            self.assertEqual(item['INCHI_KEY'], "LFQSCWFLJHTTHZ-UHFFFAOYSA-N")

    @patch("rxn_rdf_converter.reactionkg.message_to_row")
    def test_generate_compound_identifiers_from_existing_inchikey(self, mock_msg_to_row):
        id_pb = create_mock_pb_field_container({'type': 'INCHI_KEY', 'value': 'EXISTING-KEY', 'details': None})
        mock_msg_to_row.return_value = {'type': 'INCHI_KEY', 'value': 'EXISTING-KEY'}

        id_list, inchi_key = self.kg.generate_compound_identifiers([id_pb])
        self.assertEqual(inchi_key, "EXISTING-KEY")
        self.assertEqual(len(id_list), 1)

    # -----------------------------------------------------------------
    # Reaction Identifiers Processing Tests
    # -----------------------------------------------------------------

    def test_process_reaction_identifiers_known_and_custom(self):
        self.kg.reaction_identifiers = [
            {
                'reactionID': self.kg.reaction_id,
                'type': 'REACTION_SMILES',
                'value': 'C=C.Br>>CCBr',
                'details': ' standard ',
                'is_mapped': True
            },
            {
                'reactionID': self.kg.reaction_id,
                'type': 'CUSTOM',
                'value': 'CustomRxnID-01'
            }
        ]
        self.kg._process_reaction_identifiers()

        # Known identifier creates an instance & links via designates
        self.assertEqual(len(self.kg.instance_dict['designates']), 1)
        self.assertEqual(self.kg.instance_dict['designates'][0][1], self.kg.chemical_reaction.iri)
        self.assertTrue(any(v[1] == 'C=C.Br>>CCBr' for v in self.kg.instance_dict['has text value']))
        self.assertTrue(any(v[1] == 'CustomRxnID-01' for v in self.kg.instance_dict['has text value']))

    # -----------------------------------------------------------------
    # Component Extraction & Amount Processing Tests
    # -----------------------------------------------------------------

    def test_extract_components_validation_errors(self):
        with self.assertRaises(ValueError):
            self.kg._extract_components({}, process_node=None, context='invalid_context')

        with self.assertRaises(ValueError):
            self.kg._extract_components({}, process_node=None, context='input')

        with self.assertRaises(ValueError):
            self.kg._extract_component_amount({}, MagicMock(), i=0, context='invalid_context')

    def test_extract_components_input_and_product(self):
        input_node = self.kg.mds.InputAddition("InputAddition.test")
        input_data = {
            'InputKey': 'aryl_halide',
            'components[0].INCHI_KEY': 'LFQSCWFLJHTTHZ-UHFFFAOYSA-N',
            'components[0].reaction_role': 'REACTANT',
            'components[0].is_limiting': True,
            'components[0].amount.type': 'moles',
            'components[0].amount.moles.value': 2.5,
            'components[0].amount.moles.units': 'MILLIMOLE',
            'components[0].identifiers[0].type': 'SMILES',
            'components[0].identifiers[0].value': 'CCO',
            'components[0].identifiers[0].details': 'anhydrous'
        }

        self.kg._extract_components(input_data, process_node=input_node, context='input')

        self.assertEqual(len(self.kg.instance_dict['member part of']), 1)
        self.assertEqual(len(self.kg.instance_dict['is limiting']), 1)
        self.assertTrue(any(v[1] == 2.5 for v in self.kg.instance_dict['has decimal value']))
        self.assertTrue(any(v[1] == 'CCO' for v in self.kg.instance_dict['has text value']))

        # Product context
        product_data = {
            'Index': 0,
            'products[0].INCHI_KEY': 'PROD-INCHI-KEY',
            'products[0].reaction_role': 'PRODUCT',
            'products[0].is_desired_product': True,
            'products[0].isolated_color': 'white solid'
        }
        _, prod_dict = self.kg._extract_components(product_data, context='product')
        self.assertIn(0, prod_dict)
        self.assertEqual(len(self.kg.instance_dict['is desired product']), 1)
        self.assertEqual(self.kg.instance_dict['isolated color'][0][1], 'white solid')

    # -----------------------------------------------------------------
    # Temperature Processing Tests
    # -----------------------------------------------------------------

    def test_process_temperature_contexts_and_errors(self):
        with self.assertRaises(ValueError):
            self.kg._process_temperature({}, context='bad_context')

        with self.assertRaises(ValueError):
            self.kg._process_temperature({}, reaction_component=None, context='input')

        cond_temp_data = {
            'temperature.setpoint.value': 80.0,
            'temperature.setpoint.units': 'CELSIUS',
            'temperature.measurements[0].type': 'THERMOCOUPLE',
            'temperature.measurements[0].details': 'internal probe',
            'temperature.measurements[0].temperature.value': 79.5,
            'temperature.measurements[0].temperature.units': 'CELSIUS',
            'temperature.measurements[0].time.value': 30.0,
            'temperature.measurements[0].time.units': 'MINUTE',
        }
        self.kg._process_temperature(cond_temp_data, context='condition')

        self.assertEqual(len(self.kg.instance_dict['bearer of']), 1)
        self.assertTrue(any(v[1] == 80.0 for v in self.kg.instance_dict['has decimal value']))
        self.assertTrue(any(v[1] == 79.5 for v in self.kg.instance_dict['has decimal value']))
        self.assertTrue(any(v[1] == 30.0 for v in self.kg.instance_dict['has datetime value']))

    # -----------------------------------------------------------------
    # Conditions, Notes, Workups, Outcomes, Setup Tests
    # -----------------------------------------------------------------

    def test_process_reaction_conditions(self):
        self.kg.reaction_conditions = [{
            'temperature': True,
            'temperature.setpoint.value': 25.0,
            'temperature.setpoint.units': 'CELSIUS',
            'pressure': True,
            'pressure.setpoint.value': 1.0,
            'pressure.setpoint.units': 'ATMOSPHERE',
            'pressure.control.type': 'SEALED',
            'pressure.atmosphere.type': 'NITROGEN',
            'stirring': True,
            'stirring.type': 'STIR_BAR',
            'stirring.rate.type': 'HIGH',
            'illumination': True,
            'illumination.type': 'LED',
            'illumination.peak_wavelength.value': 450.0,
            'illumination.peak_wavelength.units': 'NANOMETER',
            'electrochemistry': True,
            'electrochemistry.type': 'CONSTANT_CURRENT',
            'electrochemistry.details': ' undivided cell ',
            'electrochemistry.anode_material': 'carbon',
            'electrochemistry.cathode_material': 'platinum',
            'flow': True,
            'flow.type': 'PLUG_FLOW_REACTOR',
            'flow.details': ' residence time 5 min ',
            'flow.pump_type': 'syringe',
            'flow.tubing.type': 'PTFE',
            'flow.tubing.details': '1/16 inch'
        }]

        self.kg._process_reaction_conditions()

        self.assertEqual(len(self.kg.instance_dict['is made of']), 1)
        self.assertGreaterEqual(len(self.kg.instance_dict['has process part']), 4)
        self.assertTrue(any(v[1] == 450.0 for v in self.kg.instance_dict['has decimal value']))

    def test_process_reaction_notes(self):
        self.kg.reaction_notes = [{
            'is_heterogeneous': True,
            'forms_precipitate': False,
            'is_exothermic': True,
            'offgasses': True,
            'is_sensitive_to_moisture': True,
            'is_sensitive_to_oxygen': False,
            'is_sensitive_to_light': False,
            'safety_notes': 'Use blast shield',
            'procedure_details': 'Add dropwise over 10 min'
        }]
        self.kg._process_reaction_notes()

        self.assertEqual(self.kg.instance_dict['is exothermic'][0][1], True)
        self.assertEqual(self.kg.instance_dict['safety notes'][0][1], 'Use blast shield')
        self.assertEqual(self.kg.instance_dict['procedure details'][0][1], 'Add dropwise over 10 min')

    def test_process_reaction_workups(self):
        self.kg.reaction_workups = [
            {
                'Index': 0,
                'type': 'WAIT',
                'details': 'Cool to room temperature',
                'duration.value': 1.0,
                'duration.units': 'HOUR',
                'input': False,
                'stirring': False
            },
            {
                'Index': 1,
                'type': 'EXTRACTION',
                'details': 'Extract with EtOAc',
                'keep_phase': 'organic',
                'input': False,
                'stirring': False
            },
            {
                'Index': 2,
                'type': 'STIRRING',
                'details': None,
                'stirring': True,
                'stirring.rate.type': 'MEDIUM',
                'input': False
            },
            {
                'Index': 3,
                'type': 'FLASH_CHROMATOGRAPHY',
                'details': 'Hexanes/EtOAc',
                'is_automated': True,
                'input': False,
                'stirring': False
            }
        ]
        self.kg._process_reaction_workups()

        self.assertEqual(len(self.kg.instance_dict['precedes']), 4)
        self.assertEqual(self.kg.instance_dict['keep phase'][0][1], 'organic')
        self.assertEqual(self.kg.instance_dict['is automated'][0][1], True)

    def test_process_reaction_outcomes_and_measurements(self):
        self.kg.reaction_outcomes = [{
            'reactionID': self.kg.reaction_id,
            'Index': 0,
            'reaction_time': True,
            'reaction_time.value': 12.0,
            'reaction_time.units': 'HOUR',
            'products[0].INCHI_KEY': 'PROD-KEY-1',
            'products[0].reaction_role': 'PRODUCT',
            'products[0].measurements[0].type': 'YIELD',
            'products[0].measurements[0].analysis_key': 'lc_analysis',
            'products[0].measurements[0].percentage.value': 88.5,
            'products[0].measurements[0].retention_time.value': 4.2,
            'products[0].measurements[0].retention_time.units': 'MINUTE',
            'products[0].measurements[0].uses_internal_standard': True,
            'products[0].measurements[1].type': 'AMOUNT',
            'products[0].measurements[1].amount.value': 150.0,
            'products[0].measurements[1].amount.units': 'MILLIGRAM',
            'analyses': True,
            'analyses["lc_analysis"].type': 'LC',
            'analyses["lc_analysis"].details': '254 nm',
            'analyses["lc_analysis"].is_of_isolated_species': True,
            'analyses["lc_analysis"].instrument_manufacturer': 'Agilent',
            'analyses["lc_analysis"].data["peak_area"].float_value.value': 1234.5,
            'analyses["lc_analysis"].data["peak_area"].description': 'main peak'
        }]

        self.kg._process_reaction_outcomes()

        self.assertTrue(any(v[1] == 88.5 for v in self.kg.instance_dict['has decimal value']))
        self.assertTrue(any(v[1] == 150.0 for v in self.kg.instance_dict['has decimal value']))
        self.assertTrue(any(v[1] == 1234.5 for v in self.kg.instance_dict['has decimal value']))
        self.assertEqual(self.kg.instance_dict['uses internal standard'][0][1], True)
        self.assertEqual(self.kg.instance_dict['is of isolated species'][0][1], True)

    def test_process_reaction_setup(self):
        self.kg.reaction_setup = [{
            'reactionID': self.kg.reaction_id,
            'is_automated': True,
            'automation_platform': 'Chemspeed SWING XL',
            'automation_code["script_1"]': 'run_protocol.py',
            'vessel': True,
            'vessel.type': 'ROUND_BOTTOM_FLASK',
            'vessel.details': 'three-necked',
            'vessel.vessel_id': 'flask_01',
            'vessel.material.type': 'GLASS',
            'vessel.material.details': 'borosilicate',
            'vessel.volume.value': 100.0,
            'vessel.volume.units': 'MILLILITER',
            'vessel.preparations[0].type': 'OVEN_DRIED',
            'vessel.preparations[0].details': '120 C overnight',
            'vessel.preparations[1].type': 'PURGED',
            'vessel.preparations[1].details': '3x vacuum/N2',
            'vessel.attachments[0].type': 'SEPTUM',
            'vessel.attachments[0].details': 'rubber septum',
            'vessel.attachments[1].type': 'REFLUX_CONDENSER',
            'vessel.attachments[1].details': 'water-cooled',
            'environment': True,
            'environment.type': 'FUME_HOOD',
            'environment.details': 'standard lab hood'
        }]

        self.kg._process_reaction_setup()

        # Verify ReactionSetup precedes ChemicalReaction and VesselPreparations precede ReactionSetup
        self.assertEqual(len(self.kg.instance_dict['precedes']), 3)
        self.assertEqual(self.kg.instance_dict['is automated'][0][1], True)

        # Verify Vessel Volume
        self.assertTrue(any(v[1] == 100.0 for v in self.kg.instance_dict['has decimal value']))

        # Verify Vessel Material link
        self.assertEqual(len(self.kg.instance_dict['is made of']), 1)

        # Verify Environment, Automation Platform, and Vessel text values
        text_vals = [v[1] for v in self.kg.instance_dict['has text value']]
        self.assertIn('Chemspeed SWING XL', text_vals)
        self.assertIn('ROUND_BOTTOM_FLASK', text_vals)
        self.assertIn('GLASS', text_vals)
        self.assertIn('OVEN_DRIED', text_vals)
        self.assertIn('SEPTUM', text_vals)
        self.assertIn('FUME_HOOD', text_vals)

    # -----------------------------------------------------------------
    # Graph Serialization Tests (Turtle, JSON-LD, Error Cases)
    # -----------------------------------------------------------------

    def test_generate_data_graph_turtle(self):
        self.kg.fmt = "turtle"
        self.kg.instance_dict['safety notes'].append([self.kg.chemical_reaction.iri, "Wear gloves"])

        ret = self.kg.generate_data_graph(dataset_id="ds001", save_file_path=self.temp_dir)
        self.assertIs(ret, self.kg)

        expected_file = os.path.join(self.temp_dir, "mds_dataset-ds001_reaction-999888777.ttl")
        self.assertTrue(os.path.isfile(expected_file))

        # Parse back with rdflib to verify valid Turtle output
        parsed_g = Graph()
        parsed_g.parse(expected_file, format="turtle")
        self.assertGreater(len(parsed_g), 0)

    def test_generate_data_graph_jsonld(self):
        self.kg.fmt = "json-ld"
        self.kg.instance_dict['safety notes'].append([self.kg.chemical_reaction.iri, "Exothermic"])

        ret = self.kg.generate_data_graph(dataset_id="ds001", save_file_path=self.temp_dir)
        self.assertIs(ret, self.kg)

        expected_file = os.path.join(self.temp_dir, "mds_dataset-ds001_reaction-999888777.jsonld")
        self.assertTrue(os.path.isfile(expected_file))

    def test_generate_data_graph_unsupported_format_returns_none(self):
        self.kg.fmt = "xml-invalid"
        ret = self.kg.generate_data_graph(dataset_id="ds001", save_file_path=self.temp_dir)
        self.assertIsNone(ret)


if __name__ == "__main__":
    unittest.main(verbosity=2)
