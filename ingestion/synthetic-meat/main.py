"""
Shim file for Google Cloud Functions deployment.

This file allows the Cloud Functions build process to find the entrypoint
when using a 'src' layout. The buildpack installs the project as a package,
and this file imports the actual function from within the installed package.
"""
from synthetic_meat.core import generate_and_upload

# Make the function discoverable by the Functions Framework.
# The entrypoint in Terraform ('generate_and_upload') will be found in this module.
__all__ = ["generate_and_upload"]
