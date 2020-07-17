Set a configuration value from the config file.

The ``cdp configure set`` command can be used to set a single configuration
value in the CDP config file.  The ``set`` command supports both the
*qualified* and *unqualified* config values documented in the ``get`` command
(see ``cdp configure get help`` for more information).

To set a single value, provide the configuration name followed by the
configuration value.

If the config file does not exist, one will automatically be created.  If the
configuration value already exists in the config file, it will updated with the
new configuration value.

Setting a value for the ``cdp_access_key_id`` or ``cdp_private_key`` will
result in the value being writen to the shared credentials file
(``~/.cdp/credentials``).  All other values will be written to the config file
(default location is ``~/.cdp/config``).
