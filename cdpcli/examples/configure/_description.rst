Configure CDP CLI options. If this command is run with no
arguments, you will be prompted for configuration values such as your CDP
access key ID and your CDP private key.  You can configure a named profile
using the ``--profile`` argument.

When you are prompted for information, the current value will be displayed in
``[brackets]``.  If the config item has no value, it be displayed as
``[None]``.  To keep an existing value, hit enter when prompted.

=================================
Configuration Files and Variables
=================================

Two files store configuration options. If either file does not exist when you
run this command, the CDP CLI creates it for you.

Note that the ``configure`` command only work with values from these files. It
does not use any configuration values from environment variables or the IAM
role.

The files are stored in INI format, where each section corresponds to a profile.
The special profile named "default" is used by the CDP CLI when a profile is
not provided using the ``--profile`` argument.

The **credentials file** stores credential information in the following
variables:

* **cdp_access_key_id** - The CDP access key ID part of your credentials
* **cdp_private_key** - The CDP private key part of your credentials

By default, the credentials file is found at ``~/.cdp/credentials``.

The **config file** stores other information, in the following variables:

* **cdp_region** - The region for CDP API services, possible values are:

 * us-west-1 (default value)
 * eu-1
 * ap-1
 * usg-1

* **cdp_endpoint_url** - The base URL for CDP API services
* **endpoint_url** - The base URL for other, legacy CDP API services
  (``cdp configure`` does not prompt for this value)

When using CDP Public Cloud, you do not need to provide values for these
variables. Otherwise, ask your CDP administrator for the correct URLs.

By default, the config file is found at ``~/.cdp/config``.
