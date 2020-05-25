import argparse
import getpass
import os
from cryptography.fernet import Fernet


class encrypt_password:

    def __init__(self, password, file_location=None):
        self.password = password
        if file_location != None:
            self.key_file_location = os.path.join(file_location, "key.bin")
            self.password_file_location = os.path.join(file_location, "password.bin")
        else:
            self.key_file_location = "key.bin"
            self.password_file_location = "password.bin"

    def generate_fernet_key(self):
        return Fernet.generate_key()

    def write_to_file(self, file_name, text):
        with open(file_name, 'wb') as file:
            file.write(text)

    def encrypt(self):
        key = self.generate_fernet_key()
        self.write_to_file(self.key_file_location, key)
        cipher_suite = Fernet(key)
        ciphered_text = cipher_suite.encrypt(bytes(self.password, 'utf-8'))
        self.write_to_file(self.password_file_location, ciphered_text)
        print("Key '{}' and Password '{}' files generated successfully.".format(self.key_file_location,
                                                                                self.password_file_location))


def get_password():
    boolean = True
    while (boolean):
        password = getpass.getpass("Enter Your Password: ")
        check_password = getpass.getpass("Retype Your Password: ")
        if password == "":
            print("Password should not be empty.")
        elif password == check_password:
            boolean = False
        else:
            print("Password doesn't matches. Please try again.")
    return password


def main():
    parser = argparse.ArgumentParser(description='Encrypted Password File Builder')
    parser.add_argument('-l', '--fileLocation', help='Password File Location', default=None)
    args = parser.parse_args()
    file_location = args.fileLocation
    password = get_password()
    if (file_location != None):
        e = encrypt_password(password, file_location)
    else:
        e = encrypt_password(password)
    e.encrypt()


if __name__ == "__main__":
    main()
