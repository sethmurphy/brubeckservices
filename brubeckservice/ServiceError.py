import os
class ServiceError(Exception):
    """Base class for errors in the brubeckservice package."""
    def __init__(self, message, Errors):

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)

        # Now for your custom code...
        self.Errors = Errors