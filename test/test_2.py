import logging
import os
import re
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ord_schema.message_helpers import load_message
from ord_schema.datasets import save_dataset
from ord_schema.proto import dataset_pb2

from src.rxn_rdf_converter.dataset_processor import DatasetProcessor  # Adjust module name if needed
from src.rxn_rdf_converter.reaction_KG import ReactionKG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("error.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
MDS_FILE_PATH = str(PROJECT_DIR / "MDS_Onto.owl")
DATASET_FILE_PATH = str(
    PROJECT_DIR / "ord-data-examples" / "ord_dataset-00005539a1e04c809a9a78647bea649c.pb.gz"
)

dataset = load_message(DATASET_FILE_PATH, dataset_pb2.Dataset)


def sample_random_reactions(num_test_reactions=50):
    total = len(dataset.reactions)
    count = min(num_test_reactions, total)
    indices = sorted(random.sample(range(total), count))
    return [dataset.reactions[i] for i in indices], indices, total


class TestReactionKG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_reactions, indices, total = sample_random_reactions(50)
        print(f"Sampled {len(cls.test_reactions)} reactions out of {total}: {indices}")

    def _init_ontology_or_skip(self, semi):
        if not os.path.exists(MDS_FILE_PATH):
            self.skipTest("MDS_Onto.owl not found; skipping ontology-dependent test.")
        semi._initialize_instance_dict(MDS_FILE_PATH)

    def test_generate_reaction_basic_properties(self):
        """Verify generate_reaction() populates all expected lists and attributes."""
        for pb_rxn in self.test_reactions:
            semi = ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()

            self.assertEqual(semi.reaction_pb.reaction_id, pb_rxn.reaction_id)
            self.assertIsInstance(semi.reaction_id, str)
            self.assertGreater(len(semi.reaction_id), 0)

            for attr in (
                "reaction_identifiers",
                "reaction_inputs",
                "reaction_setup",
                "reaction_conditions",
                "reaction_notes",
                "reaction_workups",
                "reaction_outcomes",
                "reaction_provenance",
            ):
                self.assertIsInstance(getattr(semi, attr), list)

            if pb_rxn.identifiers and semi.reaction_identifiers:
                iv = getattr(pb_rxn.identifiers[0], "value", None)
                if iv:
                    self.assertEqual(semi.reaction_identifiers[0].get("value"), iv)

    def test_generate_compound_identifiers_on_real_components(self):
        """Verify RDKit generates InChIKeys when SMILES or InChI identifiers are present."""
        for pb_rxn in self.test_reactions:
            semi = ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()

            for item in semi.reaction_inputs:
                for pb_input_key in semi.reaction_pb.inputs:
                    cleaned_key = re.sub(r"\s+", "", str(pb_input_key.strip()))
                    if cleaned_key == item["InputKey"]:
                        for comp in semi.reaction_pb.inputs[pb_input_key].components:
                            if comp.identifiers:
                                id_list, inchi_key = semi.generate_compound_identifiers(
                                    comp.identifiers
                                )
                                self.assertIsInstance(id_list, list)
                                has_struct = any(
                                    "SMILES" in str(idm.type).upper()
                                    or "INCHI" in str(idm.type).upper()
                                    for idm in comp.identifiers
                                )
                                if has_struct and inchi_key:
                                    self.assertIsInstance(inchi_key, str)
                                    self.assertGreater(len(inchi_key), 0)

    def test_initialize_instance_dict_and_unit_mapping(self):
        """Verify ontology loading, namespaces, QUDT unit resolution, and core instances."""
        pb_rxn = self.test_reactions[0]
        semi = ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
        self._init_ontology_or_skip(semi)

        for ns in ("mds", "cco", "obo", "qudt", "unit"):
            self.assertTrue(hasattr(semi, ns))

        # Every mapped unit must be a non-None entity with a valid .iri
        self.assertIn("GRAM", semi.unit_mapping)
        self.assertIn("CELSIUS", semi.unit_mapping)
        self.assertIn("PERCENTAGE", semi.unit_mapping)
        for unit_key, entity in semi.unit_mapping.items():
            self.assertIsNotNone(entity, f"Unit {unit_key} resolved to None")
            self.assertTrue(hasattr(entity, "iri"), f"Unit {unit_key} missing .iri")

        type_iris = [t[0] for t in semi.instance_dict["type"]]
        for inst in (
            semi.chemical_reaction,
            semi.reaction_mixture,
            semi.reaction_environment,
            semi.crude_product,
        ):
            self.assertIsNotNone(inst)
            self.assertIn(inst.iri, type_iris)

    def test_extract_index_set_behavior(self):
        """Verify _extract_index_set handles numeric, string, mixed, and unmatched keys."""
        semi = ReactionKG(self.test_reactions[0], fmt="json-ld")

        self.assertEqual(
            semi._extract_index_set(
                {"components[0]": 1, "components[2]": 1, "components[1]": 1, "other": 9},
                r"^components\[(\d+)\]",
            ),
            [0, 1, 2],
        )
        self.assertEqual(
            set(
                semi._extract_index_set(
                    {"products[foo]": 2, "products[bar]": 1, "products[baz]": 1},
                    r"^products\[(.+)\]",
                )
            ),
            {"foo", "bar", "baz"},
        )
        self.assertEqual(
            semi._extract_index_set(
                {"components[0]": 1, "components[bar]": 1, "components[3]": 1},
                r"^components\[(.+)\]",
            ),
            [0, 3],
        )
        self.assertEqual(
            semi._extract_index_set({"x": 1}, r"^components\[(\d+)\]"),
            [],
        )

    def test_process_reaction_identifiers_real(self):
        """Verify _process_reaction_identifiers records all identifier values."""
        tested_any = False
        for pb_rxn in self.test_reactions:
            semi = ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            if not semi.reaction_identifiers:
                continue

            self._init_ontology_or_skip(semi)
            semi._process_reaction_identifiers()
            tested_any = True

            recorded_values = [
                tv for _, tv in semi.instance_dict.get("has text value", [])
            ]
            for ident in semi.reaction_identifiers:
                val = ident.get("value")
                if val:
                    self.assertIn(val, recorded_values)

        if not tested_any:
            self.skipTest("No reaction_identifiers found in the sampled reactions.")

    def test_extract_component_amount_specific_value(self):
        """Verify _extract_component_amount creates decimal value and unit triples."""
        semi = ReactionKG(self.test_reactions[0], fmt="json-ld").generate_reaction()
        self._init_ontology_or_skip(semi)

        component_list = {
            "InputKey": "m1",
            "components[0].amount.type": "mass",
            "components[0].amount.mass.value": 25.0,
            "components[0].amount.mass.units": "MILLIGRAM",
        }
        reaction_component = semi.mds.Component(
            f"Component#{semi.reaction_id}_m1_component_0"
        )
        semi._extract_component_amount(
            component_list, reaction_component, i=0, context="input"
        )

        self.assertTrue(
            any(25.0 == v for _, v in semi.instance_dict.get("has decimal value", []))
        )
        unit_list = semi.instance_dict.get("uses measurement unit", [])
        self.assertTrue(any("MilliGM" in str(u[1]) for u in unit_list))

    def test_extract_compound_identifiers_and_details(self):
        """Verify _extract_compound_identifiers populates text values and details."""
        semi = ReactionKG(self.test_reactions[0], fmt="json-ld").generate_reaction()
        self._init_ontology_or_skip(semi)

        component_list = {
            "InputKey": "m1",
            "components[0].identifiers[0].type": "SMILES",
            "components[0].identifiers[0].value": "CCO",
            "components[0].identifiers[0].details": "ethanol-smiles",
            "components[0].identifiers[1].type": "UNSPECIFIED",
            "components[0].identifiers[1].value": "custom-id",
        }
        reaction_component = semi.mds.Component(
            f"Component#{semi.reaction_id}_m1_component_0"
        )
        semi._extract_compound_identifiers(
            component_list, 0, "input", "components", reaction_component
        )

        values = [v for _, v in semi.instance_dict.get("has text value", [])]
        self.assertIn("CCO", values)
        self.assertIn("custom-id", values)

        details = [v for _, v in semi.instance_dict.get("details", [])]
        self.assertTrue(any("ethanol-smiles" in d for d in details))

    def test_process_temperature_condition_and_input(self):
        """Verify _process_temperature in both condition and input contexts."""
        semi = ReactionKG(self.test_reactions[0], fmt="json-ld").generate_reaction()
        self._init_ontology_or_skip(semi)

        semi._process_temperature(
            {
                "temperature.setpoint.value": 60.0,
                "temperature.setpoint.units": "CELSIUS",
                "temperature": True,
            },
            context="condition",
        )
        self.assertTrue(
            any(60.0 == v for _, v in semi.instance_dict.get("has decimal value", []))
        )

        reaction_component = semi.mds.Component(
            f"Component#{semi.reaction_id}_fake_component_0"
        )
        semi._process_temperature(
            {
                "addition_temperature.value": -5.0,
                "addition_temperature.units": "CELSIUS",
                "addition_temperature.control.type": "thermostat",
            },
            reaction_component=reaction_component,
            context="input",
        )
        self.assertTrue(
            any(-5.0 == v for _, v in semi.instance_dict.get("has decimal value", []))
        )

    def test_end_to_end_graph_generation_jsonld_and_turtle(self):
        """Integration test: generate instances and serialize both JSON-LD and Turtle graphs."""
        if not os.path.exists(MDS_FILE_PATH):
            self.skipTest("MDS_Onto.owl not found; skipping graph generation test.")

        with tempfile.TemporaryDirectory() as tmp_dir:
            for pb_rxn in self.test_reactions[:10]:
                for fmt, ext in (("json-ld", ".jsonld"), ("turtle", ".ttl")):
                    semi = ReactionKG(pb_rxn, fmt=fmt).generate_reaction()
                    semi.generate_instances(MDS_FILE_PATH)
                    result = semi.generate_data_graph("testdataset", tmp_dir)

                    self.assertIsNotNone(result)
                    expected_file = os.path.join(
                        tmp_dir, f"mds_dataset-testdataset_reaction-{semi.reaction_id}{ext}"
                    )
                    self.assertTrue(os.path.exists(expected_file))
                    self.assertGreater(len(semi.graph), 0)


class TestDatasetProcessor(unittest.TestCase):
    def _write_mini_dataset(self, folder_path, num_reactions=5):
        mini_ds = dataset_pb2.Dataset()
        mini_ds.dataset_id = dataset.dataset_id
        for rxn in dataset.reactions[:num_reactions]:
            mini_ds.reactions.append(rxn)

        ds_path = os.path.join(folder_path, "mini_dataset.pb.gz")
        save_dataset(mini_ds, ds_path)
        return ds_path

    def test_initialization_and_logger_creation(self):
        """Verify DatasetProcessor creates output directory, dataset_id, and log file."""
        with tempfile.TemporaryDirectory() as tmp_root:
            input_dir = os.path.join(tmp_root, "batch_01")
            output_dir = os.path.join(tmp_root, "output")
            log_dir = os.path.join(tmp_root, "logs")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(log_dir, exist_ok=True)

            ds_path = self._write_mini_dataset(input_dir, num_reactions=3)
            processor = DatasetProcessor(
                dataset_pb2, ds_path, MDS_FILE_PATH, output_dir, log_dir, fmt="json-ld"
            )

            expected_id = re.split("-", dataset.dataset_id)[1]
            self.assertEqual(processor.dataset_id, expected_id)
            self.assertEqual(processor.output_dir, os.path.join(output_dir, "batch_01"))
            self.assertTrue(os.path.isdir(processor.output_dir))

            expected_log = os.path.join(log_dir, f"dataset_{expected_id}.log")
            self.assertTrue(os.path.exists(expected_log))
            processor.log_handler.close()

    def test_extract_reaction_batch_processing(self):
        """Verify extract_reaction processes reactions, writes graph files, and tracks results."""
        if not os.path.exists(MDS_FILE_PATH):
            self.skipTest("MDS_Onto.owl not found; skipping batch extraction test.")

        with tempfile.TemporaryDirectory() as tmp_root:
            input_dir = os.path.join(tmp_root, "batch_02")
            output_dir = os.path.join(tmp_root, "output")
            log_dir = os.path.join(tmp_root, "logs")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(log_dir, exist_ok=True)

            ds_path = self._write_mini_dataset(input_dir, num_reactions=5)
            processor = DatasetProcessor(
                dataset_pb2, ds_path, MDS_FILE_PATH, output_dir, log_dir, fmt="json-ld"
            )

            _, reaction_errors, processed_list = processor.extract_reaction()
            processor.log_handler.close()

            self.assertEqual(reaction_errors, [])
            self.assertEqual(len(processed_list), 5)

            generated_files = os.listdir(processor.output_dir)
            self.assertEqual(len(generated_files), 5)
            self.assertTrue(all(f.endswith(".jsonld") for f in generated_files))

    def test_extract_reaction_records_errors_without_crashing(self):
        """Verify extract_reaction catches per-reaction exceptions and populates reaction_error."""
        with tempfile.TemporaryDirectory() as tmp_root:
            input_dir = os.path.join(tmp_root, "batch_03")
            output_dir = os.path.join(tmp_root, "output")
            log_dir = os.path.join(tmp_root, "logs")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(log_dir, exist_ok=True)

            ds_path = self._write_mini_dataset(input_dir, num_reactions=3)
            processor = DatasetProcessor(
                dataset_pb2, ds_path, MDS_FILE_PATH, output_dir, log_dir, fmt="turtle"
            )

            with patch.object(
                ReactionKG,
                "generate_reaction",
                side_effect=RuntimeError("Simulated parse failure"),
            ):
                _, reaction_errors, processed_list = processor.extract_reaction()

            processor.log_handler.close()

            self.assertEqual(len(reaction_errors), 3)
            self.assertEqual(processed_list, [])
            self.assertIn("Simulated parse failure", reaction_errors[0][1])


if __name__ == "__main__":
    unittest.main()