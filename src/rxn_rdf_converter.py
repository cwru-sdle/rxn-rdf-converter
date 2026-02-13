# Class and method constructions
# =================================================================
#               IMPORT REQUIREMENTS
# =================================================================
import ord_schema
from ord_schema.message_helpers import load_message, write_message, message_to_row
from ord_schema.proto import dataset_pb2, reaction_pb2
import os
from rdkit import Chem
import re
from owlready2 import get_namespace, get_ontology, Thing
import rdflib
from rdflib import Graph, RDF, RDFS, OWL, Namespace, Literal, URIRef
from rdflib.namespace import RDFS, XSD, URIRef, OWL, SKOS, PROV
import logging
import csv
from reaction_KG import ReactionKG

# =================================================================
#               NAMESPACE DEFINITIONS
# =================================================================
global AFE, AFR, AFRL, AFQ, OBO, CCO, MDS, NCIT, QUDT, UNIT

AFE = Namespace('http://purl.allotrope.org/ontologies/equipment#')
AFR = Namespace('http://purl.allotrope.org/ontologies/result#')
AFRL = Namespace('http://purl.allotrope.org/ontologies/role#')
AFQ = Namespace('http://purl.allotrope.org/ontologies/quality#')
AFM = Namespace('http://purl.allotrope.org/ontologies/material#')
OBO = Namespace('http://purl.obolibrary.org/obo/')
CCO = Namespace('https://www.commoncoreontologies.org/')
MDS = Namespace('https://cwrusdle.bitbucket.io/mds/')
NCIT = Namespace('http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#')
QUDT = Namespace('http://qudt.org/schema/qudt/')
UNIT = Namespace('http://qudt.org/vocab/unit/')

# Set up logging
logger = logging.getLogger(__name__)

# =================================================================
#               CLASS & METHOD CONSTRUCTION
# =================================================================
        
class DatasetProcessor: 
    """
    A class to process an Open Reaction Database (ORD) dataset

    Attributes: 
        dataset (:class:`dataset_pb.Dataset`): The loaded ORD dataset protocol buffer object. 
        dataset_id (str): The unique ID of the dataset, extracted from the full dataset ID.
        output_dir (str): The directory path where the processed reaction data will be saved.
        owl_onto (str): The file path to the MDS-Onto OWL ontology file 
        error_log_directory (str): The directory where dataset-specific log files are created.
        fmt (str): Format of the file containing the reaction's final data graph (e.g., "turtle" is the default)
        dataset_logger (logging.Logger): The logger configured specifically for this dataset.
        handler (logging.FileHandler): The file handler for the dataset logger. 

    """
    def __init__ (self, dataset_pb, dataset_file_path, owl_onto_file_path, output_directory, error_log_directory, fmt="turtle"):
        """Constructor method.

        Initializes the processor, loads the dataset, sets up directory paths, and creates a dataset-specific logger.

        Args: 
            dataset_pb (module): The module containing the ORD protocol buffer definitions (e.g. `ord_schema.proto.dataset_pb`).
            dataset_file_path (str): The file path to the ORD dataset file (e.g., a `.pb` or `.pb.gz` file).
            owl_onto_file_path (str): The file path to the MDS-Onto OWL ontology file used as the semantic model.
            output_directory (str): The root directory where processed data folders will be created to store dataset JSON-LD or Turtle files.
            error_log_directory (str): The directory where error log files for each dataset will be stored.
            fmt (str, optional): Format for the file containing the reaction's final data graph. Defaults to "turtle".
        """
        print(f"attempting to load_message({dataset_file_path}, dataset_pb.DataSet_")
        self.dataset = load_message(dataset_file_path, dataset_pb.Dataset,)
        self.dataset_id = re.split('-', self.dataset.dataset_id)[1]
        dataset_folder = os.path.basename(os.path.dirname(dataset_file_path))
        os.makedirs(os.path.join(output_directory, dataset_folder), exist_ok=True)
        self.output_dir = os.path.join(output_directory, dataset_folder)
        self.owl_onto = owl_onto_file_path
        self.error_log_directory = error_log_directory
        self.fmt = fmt

        # Create dataset-specific logger
        self.dataset_logger, self.log_handler = self._create_dataset_logger()

        # Log initialization
        self.dataset_logger.info(f"DatasetProcessor initiated for dataset: {self.dataset_id}")
        self.dataset_logger.info(f"Dataset file: {dataset_file_path}")
        self.dataset_logger.info(f"Output Directory: {self.output_dir}")
        self.dataset_logger.info(f"Format: {fmt}")
    
    def _create_dataset_logger(self):
        """Create simple logger for each dataset
        
        The logger's name and log file are dynamically based on :attr:`self.dataset_id`.

        Returns: 
            tuple[logging.Logger, logging.FileHandler]: a tuple containing the configured logger and the file hander.
            - dataset_logger: the configured logger object, named dynamically based on self.dataset_id, used to record messages (Type: logging.Logger)
            - handler: a file handler object that directs log records to the specific file path (log_file). (Type: logging.FileHandler)

        """

        log_file = f'{self.error_log_directory}/dataset_{self.dataset_id}.log'
        logger_name = f'dataset_{self.dataset_id}'

        # Create logger
        dataset_logger = logging.getLogger(logger_name)
        dataset_logger.setLevel(logging.INFO)
        dataset_logger.handlers.clear() # clear existing handlers

        # Create file handler
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        dataset_logger.addHandler(handler)

        return dataset_logger, handler
    
    def extract_reaction(self, dataset_reaction_list=None):
        """A method to process reactions by instantiating the ReactionKG class. 
        
        Iterates over all reactions in the dataset, attempts to process each one individually, and logs successful or failed attempts.

        Args:
            dataset_reaction_list (list, optional): A list to which the reaction IDs and their 
                corresponding dataset IDs for all successfully processed reactions will be appended. 
                Defaults to an empty list (`[]`).
        Returns: 
            tuple: A tuple containing: 
                - self: The instance of the DatasetProcessor class. (Type: DatasetProcessor)
                - reaction_error (list): A list of individual reactions and the error(s) that occurred during processing, where each element is a list like `[reaction_id, error_message]`. (Type: list)
                - dataset_reaction_list (list): The list containing `[dataset_id, reaction_id]` pairs for successfully processed reactions. (Type: list)
        """

        if dataset_reaction_list is None: 
            dataset_reaction_list = []

        reaction_error = []
        logger.info(f"Found {len(self.dataset.reactions)} reactions in {self.dataset_id}")

        for ind, reaction in enumerate(self.dataset.reactions): 
            try: 
                self.dataset_logger.info(f"Processing reaction {ind}/{len(self.dataset.reactions)}: {reaction.reaction_id}")

                reaction_data = ReactionKG(reaction, self.fmt).generate_reaction().generate_instances(self.owl_onto).generate_data_graph(self.dataset_id, self.output_dir)
                dataset_reaction_list.append([self.dataset_id, reaction.reaction_id])

                self.dataset_logger.info(f"Successfully processed reaction: {reaction.reaction_id}")
                
            except Exception as e: 
                reaction_error.append([reaction.reaction_id, str(e)])
                self.dataset_logger.error(f"Error processing reaction {reaction.reaction_id}: {e}")
                continue
        
        self.dataset_logger.info(f"Dataset {self.dataset_id} processing completed")
        
        return self, reaction_error, dataset_reaction_list


