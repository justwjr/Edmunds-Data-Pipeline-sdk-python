"""
Edmunds.com API Python wrapper - Tests
Edmunds API Documentation: http://developer.edmunds.com/

author: Michael Bock <mbock@edmunds.com>
"""

from edmunds import Edmunds

api = Edmunds('YOUR API KEY')

# Test 1: No URL Path
assert api.make_call('') == None

# Test 2: Incorrect URL Path
assert api.make_call('ERROR') == None

# Test 3: Correct URL Path
# Check that makesCount key is in response
assert 'makesCount' in api.make_call('/api/vehicle/v2/makes/count')

print "All tests passed!"
