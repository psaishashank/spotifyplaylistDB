import requests
from base64 import b64encode

def exchange_code_for_token(client_id, client_secret, code, redirect_uri):
    token_url = 'https://accounts.spotify.com/api/token'
    client_creds = f"{client_id}:{client_secret}"
    client_creds_b64 = b64encode(client_creds.encode()).decode()

    headers = {
        "Authorization": f"Basic {client_creds_b64}"
    }
    
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        return token_data  # This dictionary includes the access token, refresh token, scope, and expires_in
    else:
        raise Exception(f"Failed to retrieve token, status code: {response.status_code}")

# Usage example (replace placeholders with actual values)
client_id = ''# 'your_client_id'
client_secret = ''#'your_client_secret'
code = ''  # This is the code you get from the query parameters in the redirect URI
redirect_uri = ''#'your_redirect_uri'

token_data = exchange_code_for_token(client_id, client_secret, code, redirect_uri)
print(token_data)
