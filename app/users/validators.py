from email_validator import EmailNotValidError
from email_validator import validate_email as ev_validate_email


def validate_email(email: str) -> [bool, str, str]:
    msg = ''
    valid = False
    try:
        valid = ev_validate_email(email=email)
        email = valid.email
    except EmailNotValidError as e:
        msg = str(e)
    return valid, msg, email
