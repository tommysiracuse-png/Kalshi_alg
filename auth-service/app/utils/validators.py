import re


def is_valid_phone(phone: str) -> tuple[bool, str]:
    """Validates and normalizes a US phone number to E.164 format (+1XXXXXXXXXX)."""
    digits = re.sub(r"\D", "", phone)

    if len(digits) == 10:
        digits = "1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        pass
    else:
        return False, "Enter a valid 10-digit US phone number"

    return True, f"+{digits}"


def mask_contact(contact: str, method_type: str) -> str:
    if method_type == "email":
        local, domain = contact.split("@")
        masked_local = local[:2] + "*" * (len(local) - 2) if len(local) > 2 else "**"
        return f"{masked_local}@{domain}"
    else:
        return f"***-***-{contact[-4:]}"
