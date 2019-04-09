"""
exception.py
~~~~~~~~

Module containing Custom Exceptions
"""

from dependencies.sendemail import send_email

"""
Commenting out "ImproperTimeFormat" Assuming that we need incorrectly formatted Preptime or Cooktime
data also to be loaded as there is partition for "unknown" - difficulty  
"""
# class ImproperTimeFormat(Exception):
#     """Raised when incorrect prepTime or CookTime format"""
#     def __init__(self, e):
#         send_email(e)


class UdfUnavailable(Exception):
    """Raised when udf specified not available"""
    def __init__(self, e):
        send_email(e)
