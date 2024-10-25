def normalize_email(email):
    # Convert the email to lowercase
    email = email.lower()

    # Split the email into local part and domain part
    local_part, domain_part = email.split('@')

    # Handle Gmail-specific normalization
    if domain_part in ['gmail.com', 'googlemail.com']:
        # Remove everything after the + in the local part (for Gmail)
        local_part = local_part.split('+')[0]
        # Remove dots from the local part (for Gmail)
        local_part = local_part.replace('.', '')
        # Set domain to 'gmail.com'
        domain_part = 'gmail.com'

    # Return the normalized email
    return f'{local_part}@{domain_part}'

if __name__ == '__main__':
    list_of_emails = ['thevesh.theva+opendosm@gmail.com', 'thevesh.theva@gmail.com', 'theveshtheva@gmail.com']
    for email in list_of_emails:
        assert normalize_email(email) == 'theveshtheva@gmail.com'