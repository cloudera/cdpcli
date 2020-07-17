Suppose you had the following config file::

    [default]
    cdp_access_key_id=default_access_key
    cdp_private_key=default_private_key

    [foobar]
    farboo=true

    [profile testing]
    cdp_access_key_id=testing_access_key
    cdp_private_key=testing_private_key

The following commands would have the corresponding output::

    $ cdp configure get cdp_access_key_id
    default_access_key

    $ cdp configure get default.cdp_access_key_id
    default_access_key

    $ cdp configure get cdp_access_key_id --profile testing
    testing_access_key

    $ cdp configure get profile.testing.cdp_access_key_id
    testing_access_key

    $ cdp configure get foobar.farboo
    true

    $ cdp configure get preview.does-not-exist
    $
    $ echo $?
    1
