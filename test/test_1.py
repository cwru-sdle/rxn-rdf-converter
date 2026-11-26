# =================================================================
#               IMPORT REQUIREMENTS
# =================================================================
from ord_schema.message_helpers import load_message
from ord_schema.proto import dataset_pb2
import os
import re
from src.rxn_rdf_converter import dataset_processor
import logging
import random
import unittest
import tempfile
from pathlib import Path

# =================================================================
#               SETUP LOGGING ERRORS
# =================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("error.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

# =================================================================
#               INITIATE FILE PATH
# =================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
mds_file_path = str(PROJECT_DIR / "MDS_Onto.owl")

dataset = load_message(
    "./ord-data-examples/ord_dataset-00005539a1e04c809a9a78647bea649c.pb.gz",
    dataset_pb2.Dataset,
)

def generate_random_reaction_set(num_test_reactions):
    num_total_reactions = len(dataset.reactions)
    print(f"There are {num_total_reactions} reactions in the file list")
    random_reaction_nums = random.sample(
        range(1, num_total_reactions + 1), num_test_reactions
    )
    random_reaction_nums.sort()
    return random_reaction_nums, num_test_reactions, num_total_reactions


def create_list_of_protocol_buffer_test_reactions(random_reaction_nums):
    current_reaction_index = 0
    current_reaction_num = 1
    protocol_buffer_test_reactions = []

    for j in range(len(dataset.reactions)):
        if current_reaction_index < len(random_reaction_nums):
            if current_reaction_num == random_reaction_nums[current_reaction_index]:
                protocol_buffer_test_reactions.append(dataset.reactions[j])
                current_reaction_index += 1
        current_reaction_num += 1

    return protocol_buffer_test_reactions


class TestRxnRdfConverter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        random_reaction_nums, _, num_total_reactions = generate_random_reaction_set(50)
        print(
            f"List of 50 random reaction numbers out of {num_total_reactions} total:"
        )
        print(random_reaction_nums)
        cls.protocol_buffer_test_reactions = (
            create_list_of_protocol_buffer_test_reactions(random_reaction_nums)
        )

    def _init_ontology_or_skip(self, semi):
        if not os.path.exists(mds_file_path):
            self.skipTest("Ontology file not available; skipping ontology-dependent tests.")
        try:
            semi._initialize_instance_dict(mds_file_path)
        except Exception as e:
            self.fail(f"_initialize_instance_dict failed unexpectedly: {e}")

    def test_generate_reaction_basic_properties(self):
        """
        Test generate_reaction() runs and populates the basic lists and attributes.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()

            assert semi.reaction_pb is not None
            assert semi.reaction_pb.reaction_id == pb_rxn.reaction_id
            assert isinstance(semi.reaction_id, str) and len(semi.reaction_id) > 0

            assert isinstance(semi.reaction_identifiers, list)
            assert isinstance(semi.reaction_inputs, list)
            assert isinstance(semi.reaction_setup, list)
            assert isinstance(semi.reaction_conditions, list)
            assert isinstance(semi.reaction_notes, list)
            assert isinstance(semi.reaction_workups, list)
            assert isinstance(semi.reaction_outcomes, list)
            assert isinstance(semi.reaction_provenance, list)

            if getattr(pb_rxn, "identifiers", None) and semi.reaction_identifiers:
                iv = getattr(pb_rxn.identifiers[0], "value", None)
                if iv:
                    assert semi.reaction_identifiers[0].get("value") == iv

    def test_generate_compound_identifiers_on_real_components(self):
        """
        For each reaction, exercise generate_compound_identifiers() on real components where available.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()

            for item in semi.reaction_inputs:
                for pb_input_key in semi.reaction_pb.inputs:
                    if getattr(pb_input_key, "strip", None):
                        if re.sub(r"\s+", "", str(pb_input_key.strip())) == item["InputKey"]:
                            pb_input = semi.reaction_pb.inputs[pb_input_key]
                            for comp in pb_input.components:
                                if comp.identifiers:
                                    id_list, inchi_key = semi.generate_compound_identifiers(
                                        comp.identifiers
                                    )
                                    assert isinstance(id_list, list)
                                    has_smiles = any(
                                        getattr(idm, "type", None)
                                        and str(idm.type).upper().endswith("SMILES")
                                        for idm in comp.identifiers
                                    )
                                    has_inchi = any(
                                        getattr(idm, "type", None)
                                        and "INCHI" in str(idm.type).upper()
                                        for idm in comp.identifiers
                                    )
                                    if (has_smiles or has_inchi) and inchi_key:
                                        assert isinstance(inchi_key, str) and len(inchi_key) > 0

    def test_initialize_instance_dict_and_core_instances(self):
        """
        Test _initialize_instance_dict: loads ontology, sets up namespaces, unit mapping,
        prop_metadata_dict, instance_dict, and core instances.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            self._init_ontology_or_skip(semi)

            assert hasattr(semi, "mds") and hasattr(semi, "cco") and hasattr(semi, "unit")
            assert "GRAM" in semi.unit_mapping and "MILLILITER" in semi.unit_mapping
            assert all(v is not None and hasattr(v, "iri") for v in semi.unit_mapping.values())

            assert isinstance(semi.prop_metadata_dict, dict)
            assert "type" in semi.prop_metadata_dict
            assert isinstance(semi.instance_dict, dict)
            assert "type" in semi.instance_dict

            assert semi.chemical_reaction is not None
            assert semi.reaction_mixture is not None
            assert semi.reaction_environment is not None
            assert semi.crude_product is not None

            type_iris = [t[0] for t in semi.instance_dict["type"]]
            assert semi.chemical_reaction.iri in type_iris
            assert semi.reaction_mixture.iri in type_iris
            assert semi.reaction_environment.iri in type_iris
            assert semi.crude_product.iri in type_iris

    def test_extract_index_set_behavior(self):
        """
        Test _extract_index_set with numeric, string, and mixed keys.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld")

            item_dict = {"components[0]": 1, "components[2]": 1, "components[1]": 1, "other": 9}
            assert semi._extract_index_set(item_dict, r"^components\[(\d+)\]") == [0, 1, 2]

            item_dict2 = {"products[foo]": 2, "products[bar]": 1, "products[baz]": 1}
            assert set(semi._extract_index_set(item_dict2, r"^products\[(.+)\]")) == {
                "foo",
                "bar",
                "baz",
            }

            mixed = {"components[0]": 1, "components[bar]": 1, "components[3]": 1}
            assert semi._extract_index_set(mixed, r"^components\[(.+)\]") == [0, 3]

            assert semi._extract_index_set({"x": 1}, r"^components\[(\d+)\]") == []

    def test_process_reaction_identifiers_real(self):
        """
        Run _initialize_instance_dict and then _process_reaction_identifiers using real identifiers.
        """
        tested_any = False
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            if not semi.reaction_identifiers:
                continue

            self._init_ontology_or_skip(semi)
            semi._process_reaction_identifiers()
            tested_any = True

            recorded_values = [tv for _, tv in semi.instance_dict.get("has text value", [])]
            for ident in semi.reaction_identifiers:
                val = ident.get("value")
                if val:
                    assert val in recorded_values, (
                        f"Expected {val!r} from {ident} in 'has text value', got {recorded_values}"
                    )

        if not tested_any:
            self.skipTest("None of the sampled reactions contained reaction_identifiers.")

    def test_extract_components_and_roles_and_amounts(self):
        """
        Run _initialize_instance_dict and _process_reaction_inputs.
        """
        tested_any = False
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            if not semi.reaction_inputs:
                continue

            self._init_ontology_or_skip(semi)
            semi._process_reaction_inputs()
            tested_any = True
            assert isinstance(semi.instance_dict, dict)

        if not tested_any:
            self.skipTest("None of the sampled reactions contained reaction_inputs.")

    def test_extract_component_amount_specific_value(self):
        """
        Verify _extract_component_amount creates 'has decimal value' and 'uses measurement unit' triples.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            self._init_ontology_or_skip(semi)

            if not semi.reaction_inputs:
                continue

            rxn_input = semi.reaction_inputs[0]
            input_key = rxn_input.get("InputKey", "m1")

            component_list = {
                "InputKey": input_key,
                "components[0].amount.type": "mass",
                "components[0].amount.mass.value": 25.0,
                "components[0].amount.mass.units": "MILLIGRAM",
            }

            reaction_component = semi.mds.Component(
                f"Component.{semi.reaction_id}_{input_key}_component_0"
            )
            semi._extract_component_amount(
                component_list, reaction_component, i=0, context="input"
            )

            assert any(
                25.0 == v for _, v in semi.instance_dict.get("has decimal value", [])
            )
            unit_list = semi.instance_dict.get("uses measurement unit", [])
            assert any("MilliGM" in str(u[1]) for u in unit_list)

    def test_extract_compound_identifiers_and_details(self):
        """
        Test _extract_compound_identifiers updates instance_dict with text value and details.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            self._init_ontology_or_skip(semi)

            if not semi.reaction_inputs:
                continue

            rxn_input = semi.reaction_inputs[0]
            input_key = rxn_input.get("InputKey", "m1")
            component_list = {
                "InputKey": input_key,
                "components[0].identifiers[0].type": "SMILES",
                "components[0].identifiers[0].value": "CCO",
                "components[0].identifiers[0].details": "ethanol-smiles",
                "components[0].identifiers[1].type": "UNSPECIFIED",
                "components[0].identifiers[1].value": "custom-id",
            }
            reaction_component = semi.mds.Component(
                f"Component.{semi.reaction_id}_{input_key}_component_0"
            )

            semi._extract_compound_identifiers(
                component_list, 0, "input", "components", reaction_component
            )

            values = [v for _, v in semi.instance_dict.get("has text value", [])]
            assert "CCO" in values
            assert "custom-id" in values

            details = [v for _, v in semi.instance_dict.get("details", [])]
            assert any("ethanol-smiles" in d for d in details)

    def test_process_reaction_inputs_and_generate_graph(self):
        """
        Integration test: generate instances and serialize a JSON-LD data graph.
        """
        if not os.path.exists(mds_file_path):
            self.skipTest("Ontology missing; skipping graph generation tests.")

        with tempfile.TemporaryDirectory() as tmp_path:
            for pb_rxn in self.protocol_buffer_test_reactions:
                semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
                semi.generate_instances(mds_file_path)
                semi.generate_data_graph("testdataset", str(tmp_path))

                files = os.listdir(tmp_path)
                assert len(files) > 0
                assert hasattr(semi, "graph")
                assert len(semi.graph) > 0

    def test_process_temperature_condition_and_input(self):
        """
        Validate _process_temperature in both condition and input contexts.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            self._init_ontology_or_skip(semi)

            cond = {
                "temperature.setpoint.value": 60.0,
                "temperature.setpoint.units": "CELSIUS",
                "temperature": True,
            }
            semi._process_temperature(cond, context="condition")
            assert any(
                60.0 == v for _, v in semi.instance_dict.get("has decimal value", [])
            )

            reaction_component = semi.mds.Component(
                f"Component.{semi.reaction_id}_fake_component_0"
            )
            add_temp = {
                "addition_temperature.value": -5.0,
                "addition_temperature.units": "CELSIUS",
                "addition_temperature.control.type": "thermostat",
            }
            semi._process_temperature(
                add_temp, reaction_component=reaction_component, context="input"
            )
            assert any(
                -5.0 == v for _, v in semi.instance_dict.get("has decimal value", [])
            )

    def test_process_reaction_conditions_full(self):
        """
        Run _process_reaction_conditions using real reaction_conditions.
        """
        tested_any = False
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            if not semi.reaction_conditions:
                continue

            self._init_ontology_or_skip(semi)
            semi._process_reaction_conditions()
            tested_any = True
            assert isinstance(semi.instance_dict, dict)

        if not tested_any:
            self.skipTest("None of the sampled reactions contained reaction_conditions.")

    def test_process_reaction_notes_and_workups_and_outcomes(self):
        """
        Run the methods that process notes, workups, and outcomes.
        """
        for pb_rxn in self.protocol_buffer_test_reactions:
            semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
            self._init_ontology_or_skip(semi)

            semi._process_reaction_notes()
            semi._process_reaction_workups()
            semi._process_reaction_outcomes()

            assert isinstance(semi.instance_dict, dict)

    def test_process_reaction_setup_and_graph_saving(self):
        """
        Process setup and ensure generate_data_graph saves an output file.
        """
        if not os.path.exists(mds_file_path):
            self.skipTest("Ontology missing; skipping setup and graph tests.")

        with tempfile.TemporaryDirectory() as tmp_path:
            for pb_rxn in self.protocol_buffer_test_reactions:
                semi = dataset_processor.ReactionKG(pb_rxn, fmt="json-ld").generate_reaction()
                semi.generate_instances(mds_file_path)
                semi._process_reaction_setup()
                semi.generate_data_graph("testdataset", str(tmp_path))

                files = os.listdir(tmp_path)
                assert len(files) > 0


if __name__ == "__main__":
    unittest.main()