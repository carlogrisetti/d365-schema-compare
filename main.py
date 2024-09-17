import os
import yaml
import adal
import time
import pandas as pd
from d365api import Client

CONFIG_FILEPATH = 'config.yaml'
RESULTS_PATH = 'results'
CLEAN_RESULTS = True  # Empties the RESULTS_PATH folder, starting each time with a clean slate
VERBOSE = False  # Just shows some verbose info about fields collected


class ClientManager:
    access_token = None
    access_token_refresh_time = None
    tenant_id = None
    client_url = None
    client_id = None
    client_secret = None
    client_object = None

    def __init__(self, tenant_id: str, client_url: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_url = client_url
        self.client_id = client_id
        self.client_secret = client_secret

    def get_access_token(self):
        if self.access_token_refresh_time is None or (time.time() - self.access_token_refresh_time) > 60:  # If N seconds have passed, get a new token
            authority_url = 'https://login.microsoftonline.com/' + self.tenant_id
            context = adal.AuthenticationContext(authority_url)
            token = context.acquire_token_with_client_credentials(
                resource=self.client_url,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            self.access_token = token["accessToken"]
            self.access_token_refresh_time = time.time()
        # In any case return the token object
        return self.access_token

    def get_client_object(self):
        self.get_access_token()
        if self.client_object is None:  # First time being called
            self.client_object = Client(domain=self.client_url, access_token=self.access_token)
        else:
            self.client_object.set_access_token(token=self.access_token)
        return self.client_object  # In any case return the client object


# Print iterations progress
def print_progress_bar(start_time: float, iteration, total, prefix='', decimals=1, length=100, fill='█', end="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        start_time  - Required  : starting date\time to compute remaining time to completion (Float)
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        end         - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percentage = (100 * (iteration / float(total)))
    percentage_str = ("{0:." + str(decimals) + "f}").format(percentage)

    elapsed_time = time.time() - start_time
    total_time = ((elapsed_time * 100) / percentage) if percentage > 0 else 0  # Avoid division by zero
    remaining_time = (total_time - elapsed_time) if (total_time - elapsed_time) >= 0 else 0
    elapsed_str = time.strftime('%M:%S', time.gmtime(elapsed_time))
    remaining_str = time.strftime('%M:%S', time.gmtime(remaining_time))
    elapsed_and_remaining = f"[{elapsed_str}-{remaining_str}]"

    speed = int((iteration / elapsed_time) * 60) if elapsed_time > 0 else 0  # Avoid division by zero
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    iter_vs_tot = f'{iteration}/{total}'
    print(f'\r{prefix} |{bar}| {iter_vs_tot.ljust(9)} ({percentage_str}%) {speed}/min {elapsed_and_remaining}', end=end)
    # Print New Line on Complete
    if iteration == total:
        print()


def get_entity_definitions(client_manager: ClientManager):
    time_start = time.time()

    client = client_manager.get_client_object()
    result = client.make_request(
        method='get',
        endpoint='EntityDefinitions',
        select='LogicalName',
        expand='Attributes',
    )

    time_end = time.time()
    time_taken = time_end - time_start
    return result, time_taken


def load_config() -> dict:

    # region Config
    # Let's start by stating some obvious facts
    print(f"==> Configuration file: {CONFIG_FILEPATH}")

    with open(CONFIG_FILEPATH) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

        if 'environments' in config:
            for environment in config['environments']:
                print(f"==> Environment configuration found: {environment}")
        else:
            raise SyntaxError("==> Configuration incorrect. Missing 'environments' key.")

        if 'baseline' in config:
            if config['baseline'] in config['environments']:
                print(f"==> Using {config['baseline']} as baseline")
            else:
                raise ValueError(f"==> Baseline environment {config['baseline']} is not in the configured environments.")
        else:
            raise SyntaxError("==> Configuration incorrect. Missing 'baseline' key.")

    # Clear or create results folder
    if os.path.isdir(RESULTS_PATH):
        if CLEAN_RESULTS:
            print(f"==> Clearing '{RESULTS_PATH}' folder")
            for file in os.listdir(RESULTS_PATH):
                os.remove(os.path.join(RESULTS_PATH, file))
    else:
        print(f"==> Creating '{RESULTS_PATH}' folder")
        os.mkdir(RESULTS_PATH)
    # endregion

    return config


def get_metadata(config: dict) -> None:
    for environment_name in config['environments']:
        environment_config = config['environments'][environment_name]
        print(f"==> ({environment_name}) Started working on environment")

        # Loading environment configuration
        tenant_id = environment_config['tenant_id']
        client_url = environment_config['client_url']
        client_id = environment_config['client_id']
        client_secret = environment_config['client_secret']

        # Logging in to the system
        print(f"==> ({environment_name}) Authenticating on Dynamics 365...")
        client_manager = ClientManager(
            tenant_id=tenant_id,
            client_url=client_url,
            client_id=client_id,
            client_secret=client_secret
        )

        # Getting entity structure
        print(f"==> ({environment_name}) Retrieving entity definitions...")
        result, time_taken = get_entity_definitions(client_manager=client_manager)
        print(f"==> ({environment_name}) It took {time_taken:.1f}s to retrieve {len(result['value'])} items")

        entity_definitions = result['value']
        entity_fields = []
        for entity in entity_definitions:
            entity_logical_name = entity['LogicalName']
            entity_attributes = entity['Attributes']
            for attribute in entity_attributes:
                attribute = dict(attribute)  # sanitize object type
                logical_name = attribute.get('LogicalName')
                attribute_type = attribute.get('AttributeType')
                max_length = attribute.get('MaxLength')
                entity_fields += [[entity_logical_name, logical_name, attribute_type, max_length]]
                if VERBOSE:
                    print(f"Entity {entity_logical_name}: {logical_name} - {attribute_type}({max_length})")

        df = pd.DataFrame.from_records(data=entity_fields, columns=['EntityName', 'ColumnName', 'ColumnType', 'ColumnLength'])
        environment_output = os.path.join(RESULTS_PATH, f"entity_fields_{environment_name}.csv")
        df.to_csv(path_or_buf=environment_output, index=False)

        print(f"==> ({environment_name}) Done!")

    return


def compare_environments(config: dict) -> None:

    # We want to compare the baseline environment...
    baseline_name = config['baseline']
    baseline_input = os.path.join(RESULTS_PATH, f"entity_fields_{baseline_name}.csv")
    baseline_df = pd.read_csv(filepath_or_buffer=baseline_input)
    baseline_df.sort_values(by=['EntityName', 'ColumnName'], ignore_index=True, inplace=True)

    # ... with all the environments, except for itself, of course.
    for environment_name in [env for env in config['environments'] if env != baseline_name]:
        print(f"==> Comparing {environment_name} to {baseline_name} ")
        environment_input = os.path.join(RESULTS_PATH, f"entity_fields_{environment_name}.csv")
        environment_df = pd.read_csv(filepath_or_buffer=environment_input)
        environment_df.sort_values(by=['EntityName', 'ColumnName'], ignore_index=True, inplace=True)

        # Calculate merged dataframe and export it to .CSV
        merged_df = environment_df.merge(right=baseline_df, how='outer', on=['EntityName', 'ColumnName'], suffixes=(f"_{environment_name}", f"_{baseline_name}"), indicator=True)
        merged_df.sort_values(by=['EntityName', 'ColumnName'], ignore_index=True, inplace=True)
        merged_output_csv = os.path.join(RESULTS_PATH, f"merged_{environment_name}_{baseline_name}.csv")
        merged_df.to_csv(path_or_buf=merged_output_csv, index=False)

        # Calculate differences dataframe
        differences_df = merged_df.drop(merged_df[
            (
                (merged_df[f"ColumnType_{environment_name}"] == merged_df[f"ColumnType_{baseline_name}"])
                |
                (merged_df[f"ColumnType_{environment_name}"].isna() & merged_df[f"ColumnType_{baseline_name}"].isna())
            )
            &
            (
                (merged_df[f"ColumnLength_{environment_name}"] == merged_df[f"ColumnLength_{baseline_name}"])
                |
                (merged_df[f"ColumnLength_{environment_name}"].isna() & merged_df[f"ColumnLength_{baseline_name}"].isna())
            )
        ].index)

        # Replace indicator labels
        differences_df = differences_df.astype({'_merge': 'str'})

        differences_df.loc[(
            (differences_df['_merge'] == 'both')
            &
            (differences_df[f"ColumnType_{environment_name}"] != differences_df[f"ColumnType_{baseline_name}"])
        ), '_merge'] = "Different Type"
        differences_df.loc[(
            (differences_df['_merge'] == 'both')
            &
            (differences_df[f"ColumnType_{environment_name}"] == differences_df[f"ColumnType_{baseline_name}"])
        ), '_merge'] = "Different Length"

        differences_df = differences_df.replace(to_replace={
            'left_only': f"Missing in {baseline_name}",  # left is environment, so missing in baseline
            'right_only': f"Missing in {environment_name}",  # right is baseline, so missing in environment
            'both': r"Different Type\Length"  # Any remaining 'both' is an error?
        })
        differences_df.rename(columns={'_merge': 'Difference'}, inplace=True)

        differences_summary = differences_df.groupby(['EntityName', 'Difference'], as_index=False)["ColumnName"].count()
        differences_summary = differences_summary.pivot_table(index='EntityName', columns='Difference', values='ColumnName', fill_value=0)
        differences_summary = differences_summary.astype(int)
        differences_summary.reset_index(inplace=True)
        differences_summary.columns.name = None  # Needed to avoid a phantom column due to pivot_table()

        # Generate HTML
        differences_output_detail = os.path.join(RESULTS_PATH, f"differences_{environment_name}_{baseline_name}_detail.html")
        differences_df.to_html(buf=differences_output_detail, na_rep='', index=False)

        differences_output_summary = os.path.join(RESULTS_PATH, f"differences_{environment_name}_{baseline_name}_summary.html")
        differences_summary.to_html(buf=differences_output_summary, na_rep='', index=False)
    return


def main():
    config = load_config()
    get_metadata(config=config)
    compare_environments(config=config)


if __name__ == '__main__':
    main()
