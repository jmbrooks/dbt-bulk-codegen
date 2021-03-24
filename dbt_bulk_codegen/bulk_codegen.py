"""
Read dbt_project file for sources, creates src file, generate model
 dbt run-operation generate_source --args '{"schema_name": "salesforce", "database_name": "fivetran_database",
 "generate_columns": True}' >  _src_salesforce_sandbox.yml

For Productionization/Open Sourcing, list of necessary next steps:
TODO: Support custom destination for generated source and base model files
TODO (DONE): Allow bulk-codegen to live in another directory inside the dbt project rather than only in dbt proj
TODO: Different method for supply source database and schema, and running bulk-codegen (e.g. CLI, config file)
TODO (DONE): If a source file already exists, we don't want to overwrite it. Allow for different options here
TODO (DONE): If a base model file already exists, we don't want to overwrite it. Align on different options here
TODO: Create setup.py and register project to PyPI
TODO: Handle for if profiles.yml is not at ~/.dbt/profiles.yml, which can be set via CLI or environment variable
TODO: Create LICENSE file, likely with Apache 2.0 License (needs confirmation w/ management)
TODO: README on the project and how it works
TODO: Basic unit tests to ensure expected behavior and signal project maturity
TODO: Evaluate supporting scanning dbt_project.yml so we don't need to hardcode reading/writing to 'models' folder
TODO: Specify data types for input params and outputs of functions
TODO: Completed Google-style docstrings per function
TODO: Full PEP-8 adherence (or run the script(s) through Black, the Python formatter)
TODO: Decide if this will love standalone or in a wider "dbtea" dbt toolkit (dbtea name could def change haha)

Items as later TODOs:
 - Create CONTRIBUTING.md doc to inform folks on how to contribute
 - Create .github folder for bug reports and pull request templates
 - Create integration tests for at least one data warehouse provider (easiest is Postgres)
 - Add CI (probably Circle) to run tests as part of deployment flow
 - Advanced: Support JUST replacing body of source YAML, so top-level metadata is maintained
 - Advanced: Support JUST adding new columns and tables to source YAMLs, so no manual work by team is lost
 - Advanced: Add new column to existing staging SQL model files when they exist, without replacing the file
"""
import logging
import os
import re
import subprocess
import yaml

from typing import List

regex_ansi_escape_sequences = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

# Creating logger here while library is used more ad-hoc and as a single module
logging.basicConfig(level=logging.INFO)


def fetch_dbt_project(custom_project_path: str = None) -> str:
    """Return path to the base of the closest dbt project by traversing from current working directory backwards in
    order to find a dbt_project.yml file.

    If an optional custom project path is specified (which should be a full path to the base project path of a dbt
    project), return that directory instead.
    """
    project_directory = os.getcwd()
    root_path = os.path.abspath(os.sep)

    if custom_project_path:
        return custom_project_path

    while project_directory != root_path:
        dbt_project_file = os.path.join(project_directory, 'dbt_project.yml')

        if os.path.exists(dbt_project_file):
            logging.info('Running bulk-codegen against dbt project at path: {}'.format(project_directory))
            return project_directory
        else:
            project_directory = os.path.dirname(project_directory)

    raise FileExistsError("No dbt_project.yml file found in current or any direct parent paths. You need to run "
                          "bulk-codegen from within a dbt project in order to use its tooling")


def run_dbt_deps(project_directory: str) -> None:
    """Run `dbt deps` command to install dbt project dependencies; the `codegen` package must be included."""
    project_packages_file = os.path.join(project_directory, 'packages.yml')
    if not os.path.isfile(project_packages_file):
        raise FileExistsError("You must have a packages.yml file specified in your project")

    with open(project_packages_file, 'r') as package_stream:
        package_data = yaml.safe_load(package_stream) or {}

    package_list = [entry.get("package") for entry in package_data.get("packages", {})]
    if "fishtown-analytics/codegen" not in package_list:
        raise ValueError("You have not brought the codegen dbt package into your project! You must include the package "
                         "'fishtown-analytics/codegen' in your `packages.yml` file to use bulk-codegen.")

    logging.info("Fetching dbt project package dependencies, including dbt codegen")
    subprocess.run(['dbt', 'deps'], check=True, cwd=project_directory)


def source_cmd_generator(db_name: str, source_list: list, generate_columns: bool = True,
                         destination_folder_path: str = '.'):
    """yaml time"""
    source_commands = []
    for source in source_list:
        source_mapping = dict()
        destination_folder = os.path.join(destination_folder_path, source)
        os.makedirs(destination_folder, exist_ok=True)

        output_file_name = f'_src_{source}.yml'
        destination_file_path = os.path.join(destination_folder, output_file_name)

        logging.info(f"Creating source YAML schema command for source schema: {source}")
        src_cmd = f"dbt run-operation generate_source --args '{{\"schema_name\": \"{source}\", \"database_name\":" \
                  f" \"{db_name}\", \"generate_columns\": {generate_columns}}}'"

        source_mapping[source] = {"destination_folder": destination_folder, "destination_file_name": output_file_name,
                                  "source_destination_path": destination_file_path, "source_command": src_cmd}
        source_commands.append(source_mapping)

    return source_commands


def src_yml_scan(source_mappings):
    """"""
    source_with_table_mappings = list()

    for source_mapping in source_mappings:
        for source_name, mapping_data in source_mapping.items():
            with open(mapping_data.get("source_destination_path"), 'r') as stream:
                data_loaded = yaml.safe_load(stream)

            tables_list = [data_loaded][0]['sources'][0]['tables']
            table_names = [{d['name']: {"file_name": os.path.join(
                mapping_data.get("destination_folder"), f"stg_{source_name}__{d['name']}.sql")}} for d in tables_list]
            mapping_data["tables"] = table_names

            source_with_table_mappings.append({source_name: mapping_data})

    # print(source_with_table_mappings)
    return source_with_table_mappings


def all_base_commands_generator(source_table_mappings):
    """Compile all base model commands per source and table within each source."""
    source_with_base_command_mappings = list()
    for source_mapping in source_table_mappings:
        for source_name, mapping_data in source_mapping.items():
            logging.info(f"Creating base model generation commands for source schema: {source_name}")
            source_base_model_commands = base_command_generator(
                source_name, mapping_data.get("tables"), destination_folder_path=f'models/{source_name}')

            source_with_base_command_mappings.append({source_name: source_base_model_commands})

    return source_with_base_command_mappings


def base_command_generator(source_name: str, table_mappings: list, destination_folder_path: str):
    """Compile all base model commands per source and table within each source."""
    table_mappings_with_base_models = list()

    for table_mapping in table_mappings:
        for table, mapping_data in table_mapping.items():
            os.makedirs(destination_folder_path, exist_ok=True)
            base_cmd = f"dbt run-operation generate_base_model --args '{{\"source_name\": \"{source_name}\"," \
                       f" \"table_name\": \"{table}\"}}'"

            mapping_data.update({"base_command": base_cmd})
            table_mapping[table] = mapping_data

        table_mappings_with_base_models.append(table_mapping)

    return table_mappings_with_base_models


def bash_run_and_make_files(cmd_type, command_mappings: List[dict], if_exists: str, project_directory: str):
    """"""
    if cmd_type == 'src':
        for mapping in command_mappings:
            for source_name, command_data in mapping.items():
                if if_exists.lower() == 'skip' and os.path.isfile(command_data.get('source_destination_path')):
                    logging.info(f"Skipping source generation for source `{source_name}` as this source YAML file "
                                 f"already exists at path: {command_data.get('source_destination_path')}")
                    continue

                logging.info(f"Running dbt run-operation generate_source for source: {source_name}...")
                raw_result = subprocess.run(command_data.get('source_command'), shell=True, check=True, text=True,
                                            cwd=project_directory, stdout=subprocess.PIPE)
                raw_result.check_returncode()
                cleaned_prefix_result = raw_result.stdout[raw_result.stdout.find('version: 2'):]
                final_source_yaml_result = regex_ansi_escape_sequences.sub('', cleaned_prefix_result)

                if if_exists.lower() == 'replace':
                    open_mode = 'w'
                else:
                    open_mode = 'a'

                logging.info(f"Creating clean dbt source YAML file for source `{source_name}` to file path: "
                             f"{command_data.get('source_destination_path')}")
                with open(command_data.get('source_destination_path'), open_mode) as source_file:
                    source_file.write(final_source_yaml_result)

    else:
        for mapping in command_mappings:
            for source_name, source_data in mapping.items():
                for table_mappings in source_data:
                    for table_name, table_data in table_mappings.items():
                        if if_exists.lower() == 'skip' and os.path.isfile(table_data.get("file_name")):
                            logging.info(f"Skipping model for table `{table_name}` in source {source_name} as its "
                                         f"model SQL file already exists at path: {table_data.get('file_name')}")
                            continue

                        logging.info(f"Running dbt run-operation generate_base_model for {source_name}.{table_name}...")
                        raw_result = subprocess.run(table_data.get("base_command"), shell=True, check=True, text=True,
                                                    cwd=project_directory, stdout=subprocess.PIPE)
                        raw_result.check_returncode()
                        cleaned_prefix_result = raw_result.stdout[raw_result.stdout.find('with source as'):]
                        final_base_model_result = regex_ansi_escape_sequences.sub('', cleaned_prefix_result)

                        if if_exists.lower() == 'replace':
                            open_mode = 'w'
                        else:
                            open_mode = 'a'

                        logging.info(f"Creating dbt model staging SQL file for table `{table_name}` in source "
                                     f"`{source_name}` to file path: {table_data.get('file_name')}")
                        with open(table_data.get("file_name"), open_mode) as source_file:
                            source_file.write(final_base_model_result)


def main():
    """Primary called entrypoint."""
    source_database_name = 'lake'
    source_schemas_list = ['fivetran_log', 'sales']
    custom_dbt_project_path = None  # Only necessary if not running script from within a dbt project
    if_exists_behavior = 'skip'  # Either 'replace', 'append', or 'skip' for how to handle if file already exists

    # Fetch path to dbt project
    dbt_project_directory = fetch_dbt_project(custom_project_path=custom_dbt_project_path)
    models_directory = os.path.join(dbt_project_directory, 'models')  # Change if needed, manually for now

    # Run `dbt deps` to fetch codegen and other packages
    run_dbt_deps(dbt_project_directory)

    # Generate source command(s), and create YAML files for each source schema
    source_command_mappings = source_cmd_generator(source_database_name, source_schemas_list,
                                                   destination_folder_path=models_directory)
    bash_run_and_make_files('src', source_command_mappings, if_exists=if_exists_behavior,
                            project_directory=dbt_project_directory)

    # Modify source command mappings to include base model destination paths
    source_command_mappings = src_yml_scan(source_command_mappings)

    # Generate base staging model commands, and create the dbt staging SQL models
    source_with_base_command_mappings = all_base_commands_generator(source_command_mappings)
    bash_run_and_make_files('base', source_with_base_command_mappings, if_exists=if_exists_behavior,
                            project_directory=dbt_project_directory)

    logging.info('bulk-codegen processing completed.')


if __name__ == '__main__':
    main()
