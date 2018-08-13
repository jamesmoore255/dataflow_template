# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
from time import sleep

import apache_beam as beam
import firebase_admin
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from beam_utils.sources import CsvFileSource
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT = ""


class RefactorDict(beam.DoFn):
    def __init__(self, n):
        self._n = n
        self._buffer = []

    def process(self, element):
        """
        Validate all elements
        Validate and format phone number
        If no phone number, delete element
        If phone number invalid, delete element
        If any sub-element is NoneType, do not append to dictionary
        :param element: Dictionary read from CSV file
        :return: Formatted list of Dicts with validated values
        """
        import dateinfer
        import datetime
        import re
        import phonenumbers

        def represents_int(n):
            try:
                int(n)
                return True
            except ValueError:
                return False

        if 'firstname' in element:
            element['first_name'] = element['firstname']

        if 'lastname' in element:
            element['last_name'] = element['lastname']

        if 'birth_date' in element:
            element['birthdate'] = element['birth_date']

        # Confirm, if items have no information, make NoneType
        if 'first_name' not in element or element['first_name'] is "":
            element['first_name'] = None

        if 'last_name' not in element or element['last_name'] is "":
            element['last_name'] = None

        if 'language' not in element or element['language'] is "":
            element['language'] = None

        if 'birthdate' not in element or element['birthdate'] is "":
            element['birthdate'] = None

        # This function checks the format of the birthdate to then parse the correct format
        if element['birthdate'] is not None:

            birth_dates = []

            for x in xrange(0, 50):
                birth_dates.append(element['birthdate'])

            new_dates = [d.replace("-", "/") for d in birth_dates]

            date_inferance = dateinfer.infer(new_dates)

        phone_number = element['phone_number']

        if represents_int(phone_number):
            parse_phone = phonenumbers.parse(phone_number, "US")
            valid_phone = phonenumbers.is_valid_number(parse_phone)
            if valid_phone is False:
                del element

            else:
                format_number = phonenumbers.format_number(parse_phone, phonenumbers.PhoneNumberFormat.E164)

                dictionary = {
                              'backup': False,
                              }

                if element['first_name'] is not None:
                    dictionary[u'firstName'] = unicode(element[u'first_name'].title(), "utf-8")

                if element['last_name'] is not None:
                    dictionary[u'lastName'] = unicode(element[u'last_name'].title(), "utf-8")

                if element['language'] is not None:
                    dictionary[u'language'] = unicode(element[u'language'].lower(), "utf-8")

                if element['birthdate'] is not None:
                    birth_date = element['birthdate'].replace("-", "/")
                    proper_date = datetime.datetime.strptime(birth_date,
                                                             date_inferance).strftime('%m/%d/%Y')
                    month, day, year = re.split('\W+', proper_date)
                    dictionary[u'birthdate'] = {
                        u'day': int(day),
                        u'month': int(month),
                        u'year': int(year)
                    }

                self._buffer.append([
                    (format_number, dictionary)
                ])

        else:
            del element

        if len(self._buffer) == self._n:
            logging.info("buffer length: {}".format(len(self._buffer)))
            yield self._buffer
            self._buffer = []

    # If bundle is less than 10000, send as final bundle
    def finish_bundle(self):
        from apache_beam.utils import timestamp
        from apache_beam.transforms.window import WindowedValue, GlobalWindow
        if len(self._buffer) != 0:
            logging.info("Final Buffer Length: {}".format(len(self._buffer)))
            yield WindowedValue(self._buffer, timestamp.MIN_TIMESTAMP, [GlobalWindow()])
            self._buffer = []


class ContactUploadOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    path and organization parameters are necessary for execution of pipeline
    campaign is optional for committing to bigquery
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--source',
            type=str,
            help='Source name')

def run():
    """
    Pipeline entry point, runs the all the necessary processes
    - Read CSV file out as Dict
    - Format Dictionary
    - Commit to Firestore and/or BigQuery
    """

    def firestore_commit(element):
        """
        If no credentials in firebase apps, initialize
        Set Document Reference
        Batch and commit every 400 to Firestore
        """

        if not len(firebase_admin._apps):
            fire_cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(fire_cred, {
                'projectId': PROJECT
            })

        source = contact_options.source.get()

        db = firestore.client()

        batch = db.batch()

        x = 0
        y = 400
        z = len(element)

        logging.info("Length of contacts: {}".format(z))

        logging.info("First Element: {}".format(element[0]))
        for x in xrange(x, z):
            org_ref = org_base.collection(source).document(element[x][0][0])
            batch.set(org_ref, element[x][0][1])
            x += 1
            if x == y:
                batch.commit()
                batch = db.batch()
                logging.info("Firestore Batch Commit")
                y += 400
                z = z - 400
                if z < y:
                    logging.info("Firestore Final Batch Commit")
                    y += z
                    z = 0
                sleep(.9)  # Time in seconds

    def bigquery_commit(element):
        """
        Initialize bigquery client
        Specify dataset, table and schema
        Append keys to key list
        Commit batches to bigquery in stream format
        """

        source = contact_options.source.get()

        logging.info("Element BigQuery length: {}".format(len(element)))

        client = bigquery.Client(project=PROJECT)
        dataset_ref = client.dataset('campaign_contact')
        table_ref = dataset_ref.table(source)

        schema = [
            bigquery.SchemaField('id', 'STRING', mode='REQUIRED')
        ]

        # BigQuery table checker function
        def if_tbl_exists(clients, table_refs):
            """Check if table exists, if False, create table"""
            logging.info("Check Table")
            try:
                clients.get_table(table_refs)
                return True
            except NotFound:
                return False

        if if_tbl_exists(client, table_ref) is False:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logging.info("Table created: {}".format(table_ref))
        else:
            pass

        key_data = []

        for elem in element:
            key_data.append({"id": elem[0][0]})

        if len(key_data) == len(element):
            client.insert_rows_json(table_ref, key_data,
                                    ignore_unknown_values=True, skip_invalid_rows=True)
            phone_numbers = []
            logging.info("BigQuery Batch Commit, number of elements: {}".format(len(phone_numbers)))

    global PROJECT

    # Retrieve project Id and append to PROJECT form GoogleCloudOptions
    PROJECT = PipelineOptions().view_as(GoogleCloudOptions).project

    # Initialize runtime parameters as object
    contact_options = PipelineOptions().view_as(ContactUploadOptions)

    pipeline_options = PipelineOptions()
    # Save main session state so pickled functions and classes
    # defined in __main__ can be unpickled
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Beginning of the pipeline
    p = beam.Pipeline(options=pipeline_options)

    # ---- Start of the pipeline ---- #
    lines = (p
             | beam.io.Read(CsvFileSource(contact_options.path))  # Read from runtime path provided
             | 'Refactor' >> beam.ParDo(RefactorDict(10000))  # Include number to be batched
             )

    lines | beam.FlatMap(firestore_commit)

    lines | beam.FlatMap(bigquery_commit)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()
