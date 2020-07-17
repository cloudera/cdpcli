Given an empty config file, the following commands::

    $ cdp configure set cdp_access_key_id default_access_key
    $ cdp configure set cdp_private_key default_private_key
    $ cdp configure set default.ca_bundle /path/to/ca-bundle.pem
    $ cdp configure set foobar.farboo.true

will produce the following config file::

    [default]
    ca_bundle = /path/to/ca-bundle.pem

    [foobar]
    farboo = true

and the following ``~/.cdp/credentials`` file::

    [default]
    cdp_access_key_id = default_access_key
    cdp_private_key = default_private_key
