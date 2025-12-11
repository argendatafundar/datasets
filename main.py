import dotenv
import requests

API_HOSTNAME = dotenv.dotenv_values()['API_HOSTNAME']

print(
    requests.get(
        f'{API_HOSTNAME}/raw',
        params={
            'id': 'R229C0',
        },
    ).text
)