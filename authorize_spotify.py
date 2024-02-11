import webbrowser
import urllib.parse

client_id = ''
redirect_uri = ''  # This must match the redirect URI set in your Spotify app settings
scope = 'user-library-read'  # Scope for accessing user's saved tracks

# Constructing the authorization URL
auth_url = 'https://accounts.spotify.com/authorize?' + \
    urllib.parse.urlencode({
        'response_type': 'code',
        'client_id': client_id,
        'scope': scope,
        'redirect_uri': redirect_uri,
    })

# Open the authorization URL in the default browser
'''
this should redirect to a new tab on the browser copy the code from the url
'''
webbrowser.open(auth_url)

