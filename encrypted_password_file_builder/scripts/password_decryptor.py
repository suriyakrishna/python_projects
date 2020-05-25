from cryptography.fernet import Fernet
from sys import exit


def get_key_from_file(file_location):
    try:
        with open(file_location, 'rb') as file:
            key = file.read()
        return key
    except FileNotFoundError as f:
        print("File Not Found: {}. Exiting..".format(file_location))
        exit(1)


def decrypt_password(key_file_location, password_file_location):
    cipher_suite = Fernet(get_key_from_file(key_file_location))
    unciphered_text = (cipher_suite.decrypt(get_key_from_file(password_file_location)))
    return str(unciphered_text, 'utf-8')
