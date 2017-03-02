#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Edmunds.com API Python wrapper - Media: Photos Example
Edmunds API Documentation: http://developer.edmunds.com/

author: Michael Bock <mbock@edmunds.com>
"""

import os
import sys

# import edmunds module from a different directory

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

from edmunds import Edmunds


def get_style_id(
        api,
        make,
        model,
        year,
):
    """
....Get style ID from make, model, year

....See endpoint documentation: http://developer.edmunds.com/api-documentation/vehicle/spec_model_year/v2/02_year_details/api-response.html

....:param api: The Edmunds api object with API key
....:type api: Edmunds object
....:param make:
....:type make: str
....:param model:
....:type model: str
....:param year:
....:type year: str
....:returns: Style ID or None if error
....:rtype: str or None
...."""

    endpoint = '/api/vehicle/v2/' + make + '/' + model + '/' + year
    response = api.make_call(endpoint)

    # error checking

    if not response or 'error' in response or 'errorType' in response \
            or not 'styles' in response:
        print 'Error in get_style_id'
        if 'error' in response:
            if 'message' in response['error']:
                print 'Error message:', response['error']['message']
        elif 'errorType' in response:
            print 'errorType:', response['errorType']
            if 'message' in response:
                print 'message:', response['message']
        return None

    # return first style ID
    # be careful, respoonse['id'] is the Edmunds ID, not the style ID

    return response['styles'][0]['id']


def get_photos(api, style_id):
    """
    Get photos of a vehicle by its style ID
....
....See endpoint documentation: http://developer.edmunds.com/api-documentation/vehicle/media_photos/v1/01_photos_by_styleid/api-response.html
....
....:param api: The Edmunds api object with API key
....:type api: Edmunds object
....:param style_id: Style ID of vehicle
....:type style_id: str
....:returns: Dictionary of lists, where each key is the type (subType, shotType) of photo, and each value is a list of photo urls
....:rtype: Dictionary {tuple: list} ({(subType, shotType): list_of_url_strings})
........ex: { ('exterior', 'RQ'): ['URL1', 'URL2', ...], ('interior', 'G'): ['URL1', 'URL2', ...] }
...."""

    endpoint = '/v1/api/vehiclephoto/service/findphotosbystyleid'
    response = api.make_call(endpoint, styleId=style_id)

    # error checking

    if not response or 'error' in response:
        print 'Error in get_photos'
        if not response:
            print 'No or empty response'
        elif 'error' in response:
            if 'message' in response['error']:
                print 'Error message:', response['error']['message']
        return None

    result = {}
    for obj in response:

        # key is the type of photo
        # key is a tuple of (subType, shotTypeAbbreviation)

        key = (obj['subType'], obj['shotTypeAbbreviation'])
        if key not in result:
            result[key] = []
        for url_stub in obj['photoSrcs']:
            full_url = api.BASE_MEDIA_URL + url_stub
            result[key].append(full_url)
    return result


def get_model_s_photos(api):
    """
....Get photos of the 2013 Tesla Model S

....:returns: Dictionary of Lists of photo URLS (see return value of get_photos for more info)
....:rtype: dict or None
...."""

    style_id = get_style_id(api, 'tesla', 'models', '2013')
    photo_urls = None
    if style_id:
        photo_urls = get_photos(api, style_id)
    if photo_urls:
        print photo_urls
    return photo_urls


if __name__ == '__main__':

    import yaml

    credentials = yaml.load(open(os.path.expanduser('~/edmunds_api_cred.yml')))
    creds = credentials['edmunds']

    api = Edmunds(creds.get('Key'), True)  # True indicates debug mode is ON

    print get_model_s_photos(api)
