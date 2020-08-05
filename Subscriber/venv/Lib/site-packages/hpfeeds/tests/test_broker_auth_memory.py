import unittest

from hpfeeds.broker.auth.memory import AsyncAuthenticator


async def test_find_single_key():
    a = AsyncAuthenticator({
        'test': {
            'secret': 'secret',
            'subchans': ['test-chan'],
            'pubchans': ['test-chan'],
            'owner': 'some-owner',
        }
    })

    key = await a.get_authkey('test')
    assert key['owner'] == 'some-owner'
    assert key['secret'] == 'secret'
    assert key['pubchans'] == ['test-chan']
    assert key['subchans'] == ['test-chan']


async def test_not_find_single_key():
    a = AsyncAuthenticator({
        'test': {
            'secret': 'secret',
            'subchans': ['test-chan'],
            'pubchans': ['test-chan'],
            'owner': 'some-owner',
        }
    })

    key = await a.get_authkey('test2')
    assert key == None
