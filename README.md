https://s3-us-west-1.amazonaws.com/edmundssparksubmitresults/topVehicles.html

https://www.gliffy.com/go/publish/11854628























# Edmunds API Python Wrapper

This is an awesome Python 2 wrapper for the [Edmunds.com API](http://developer.edmunds.com/api-documentation/overview/index.html).
The Edmunds.com API provides automative data including [vehicle specs](http://developer.edmunds.com/api-documentation/vehicle/),
[pricing](http://developer.edmunds.com/api-documentation/vehicle/price_tmv/v1/), [media](http://developer.edmunds.com/api-documentation/vehicle/media_photos/v1/),
[reviews](http://developer.edmunds.com/api-documentation/vehicle/content_ratings_and_reviews/v2/), and [more](http://developer.edmunds.com/api-documentation/overview/index.html#sec-6).
There are also Edmunds API endpoints for [dealership information](http://developer.edmunds.com/api-documentation/dealer/)
and [Edmunds editorial content](http://developer.edmunds.com/api-documentation/editorial/).

## Usage
Enter your [Edmunds API key](http://edmunds.mashery.com/member/register/):
```python
from edmunds import Edmunds
api = Edmunds('YOUR API KEY') # use Edmunds('YOUR API KEY', True) for debug mode
```

Make API calls to any endpoint, get a JSON object returned.
For example, get the [style details](http://developer.edmunds.com/api-documentation/vehicle/spec_style/v2/01_by_mmy/api-description.html)
for the 2011 Lexus RX 350:
```python
>>> response = api.make_call('/api/vehicle/v2/lexus/rx350/2011/styles')
>>> response
{u'styles': [{u'id': 101353967,
   u'make': {u'id': 200001623, u'name': u'Lexus', u'niceName': u'lexus'},
   u'model': {u'id': u'Lexus_RX_350',
    u'name': u'RX 350',
    u'niceName': u'rx-350'},
   u'name': u'4dr SUV (3.5L 6cyl 6A)',
   u'submodel': {u'body': u'SUV', u'modelName': u'RX 350 SUV'},
   u'trim': u'Base',
   u'year': {u'id': 100533091, u'year': 2011}},
  {u'id': 101353968,
   u'make': {u'id': 200001623, u'name': u'Lexus', u'niceName': u'lexus'},
   u'model': {u'id': u'Lexus_RX_350',
    u'name': u'RX 350',
    u'niceName': u'rx-350'},
   u'name': u'4dr SUV AWD (3.5L 6cyl 6A)',
   u'submodel': {u'body': u'SUV', u'modelName': u'RX 350 SUV'},
   u'trim': u'Base',
   u'year': {u'id': 100533091, u'year': 2011}}],
 u'stylesCount': 2}
```

Get [photos](http://developer.edmunds.com/api-documentation/vehicle/media_photos/v1/)
for the style ID 3883 (1990 Honda Civic 2dr Hatchback):
```python
>>> response = api.make_call('/v1/api/vehiclephoto/service/findphotosbystyleid', comparator='simple', styleId='3883')
>>> response
[{u'authorNames': [u'American Honda Motor Company, Inc.'],
  u'captionTranscript': u'1991 Honda Civic 2 Dr Si Hatchback',
  u'photoSrcs': [u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_131.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_396.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_300.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_400.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_500.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_185.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_175.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_196.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_423.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_276.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_87.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_150.jpg',
   u'/honda/civic/1991/oem/1991_honda_civic_2dr-hatchback_si_fq_oem_1_98.jpg'],
  u'shotTypeAbbreviation': u'FQ',
  u'subType': u'exterior',
  u'type': u'PHOTOS'}]
```

## Installation

The Edmunds API Python wrapper requires the amazing [requests library](http://docs.python-requests.org/en/latest/).
Here are the [installation instructions](http://docs.python-requests.org/en/latest/user/install/#install) and the
[source code](https://github.com/kennethreitz/requests/).

To install using pip:

```pip install edmunds``` or ```pip install git+https://github.com/EdmundsAPI/sdk-python```

Then:

```git clone https://github.com/EdmundsAPI/sdk-python.git```
or just click "Clone in Desktop" or "Download ZIP"

![Install](https://photos-2.dropbox.com/t/0/AAAwB573IRFx7wrFhdmUilGQBgIcSTEjKFIoQ2sf1XsNMw/12/16428977/png/1024x768/3/1397286000/0/2/Screenshot%202014-04-12%2013.29.55.png/Aacyj_dQNGVCjJbyiazWcdk6538H61urhhFak44v-FA)


Notes:
* Using pip to install this package will atuomatically install the requests library
* The files (edmunds.py and tests.py) will be installed to your Python site-packages folder
* It is suggested you then clone the repository (using the second command) in order to download all of the files to your computer and so that you can insert your API Key into the edmunds.py code

## Contents

```python
sdk-python/
   .gitignore
   AUTHORS.md # Info about development and how to contribute
   HISTORY.md # Version history
   LICENSE
   MANIFEST
   MANIFEST.in
   README.md # You're looking at it!
   requirements.txt
   setup.py
   edmunds/
      examples/ # Examples of using the SDK
         README.md
         media_photos.py
         spec_make.py
      __init__.py
      edmunds.py # The source code for the SDK
      tests.py
```

## Issues

Please submit any problems, requests, and comments [here](https://github.com/EdmundsAPI/sdk-python/issues).

## SDK Status

This is a beta release. We have opened sourced it at this stage to guide the development of the library and allow you to freely inspect and use the source.

## Documentation

The Edmunds API documentation can be found [here](http://developer.edmunds.com/api-documentation/overview/index.html).

## License

Licensed under the Apache v2 License.
