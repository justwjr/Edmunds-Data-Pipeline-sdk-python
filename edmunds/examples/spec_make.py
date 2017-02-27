"""
Edmunds.com API Python wrapper - Spec: Make Example
Edmunds API Documentation: http://developer.edmunds.com/

author: Michael Bock <mbock@edmunds.com>
"""

import os, sys

# import edmunds module from a different directory
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

from edmunds import Edmunds


def get_makes(api, year=None, state=None, view=None):
    """
	Get list of makes
	Documentation: http://developer.edmunds.com/api-documentation/vehicle/spec_make/v2/01_list_of_makes/api-description.html

	:param api: The Edmunds api object with API key
	:type api: Edmunds object
	:param year: 1990 - current year; optional
	:type year: str
	:param state: 'new', 'used', or 'future'; optional
	:type state: str
	:param view: 'basic' or 'full'; optional, default is basic
	:type view: str
	:returns: Makes as described here: http://developer.edmunds.com/api-documentation/vehicle/spec_make/v2/01_list_of_makes/api-response.html
	:rtype: JSON object
	"""
    endpoint = '/api/vehicle/v2/makes'
    options = {}
    if year:
        options['year'] = year
    if state:
        options['state'] = state
    if view:
        options['view'] = view

    response = api.make_call(endpoint, **options)

    # error checking
    if (not response or 'errorType' in response or
                'error' in response):
        print "Error in get_makes"
        if 'errorType' in response:
            print "errorType: " + response['errorType']
        if 'message' in response:
            print "message: " + response['message']
        return None

    return response


if __name__ == "__main__":
    import yaml

    credentials = yaml.load(open(os.path.expanduser('~/edmunds_api_cred.yml')))
    creds = credentials['edmunds']

    api = Edmunds(creds.get('Key'), True)  # True indicates debug mode is ON
    print get_makes(api, '2016', 'new', view='full')
