Configure CDP CLI options. If this command is run with no
arguments, you will be prompted for configuration values such as your CDP
access key id and you CDP private key.  You can configure a named profile
using the ``--profile`` argument.  If your config file does not exist
(the default location is ``~/.cdp/config``), the CDP CLI will create it
for you.  To keep an existing value, hit enter when prompted for the value.
When you are prompted for information, the current value will be displayed in
``[brackets]``.  If the config item has no value, it be displayed as
``[None]``.  Note that the ``configure`` command only work with values from the
config file.  It does not use any configuration values from environment
variables or the IAM role.

Note: the values you provide for the CDP Access Key ID and the CDP Private
Key will be written to the shared credentials file
(``~/.cdp/credentials``).


=======================
Configuration Variables
=======================

The following configuration variables are supported in the config file:

* **cdp_access_key_id** - The CDP access key id part of your credentials
* **cdp_private_key** - The CDP private key part of your credentials
