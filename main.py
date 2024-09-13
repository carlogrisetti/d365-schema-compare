import yaml
import adal
import time
import json
import pandas as pd
from d365api import Client

CONFIG_FILEPATH = 'config.yaml'


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


def main():

    # region Config
    with open(CONFIG_FILEPATH) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

        if 'environment' in config:
            tenant_id = config['environment']['tenant_id']
            client_url = config['environment']['client_url']
            client_id = config['environment']['client_id']
            client_secret = config['environment']['client_secret']
        else:
            raise SyntaxError("Configuration incorrect")
    # endregion

    # Let's start by stating some obvious facts
    print(f"Configuration file: {CONFIG_FILEPATH}")

    # Logging in to the system
    print("==> Authenticating on Dynamics 365...")
    client_manager = ClientManager(
        tenant_id=tenant_id,
        client_url=client_url,
        client_id=client_id,
        client_secret=client_secret
    )

    # Getting entity structure
    print("==> Retrieving Entity Definitions...")
    result, time_taken = get_entity_definitions(client_manager=client_manager)
    print(f"==> It took {time_taken:.1f}s to retrieve {len(result['value'])} items")

    entity_definitions = result['value']
    entity_fields = []
    for entity in entity_definitions:
        entity_logical_name = entity['LogicalName']
        entity_attributes = entity['Attributes']
        for attribute in entity_attributes:
            attribute = dict(attribute)  # sanitize object type
            column_number = attribute.get('ColumnNumber')
            logical_name = attribute.get('LogicalName')
            attribute_type = attribute.get('AttributeType')
            max_length = attribute.get('MaxLength')
            entity_fields += [[entity_logical_name, column_number, logical_name, attribute_type, max_length]]
            print(f"Entity {entity_logical_name} - Column {column_number}: {logical_name} - {attribute_type}({max_length})")

    df = pd.DataFrame.from_records(data=entity_fields, columns=['EntityName', 'ColumnNumber', 'ColumnName', 'ColumnType', 'ColumnLength'])

    #with open('entity_definitions.json', 'w', encoding='utf-8') as f:
    #    json.dump(entity_definitions, f, ensure_ascii=False, indent=4)

    df.to_csv(path_or_buf='entity_fields.csv', index=False)
    print("==> Done!")


if __name__ == '__main__':
    main()
