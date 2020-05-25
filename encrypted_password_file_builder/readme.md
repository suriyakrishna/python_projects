#### Encrypted Password File Builder

Encrypted Password file script will create a encrypted password file for the password provided by the user during the runtime. cryptography python module have been used to encrypt the password.

##### Script Help
```bash
$ python scripts/encrypted_password_file_builder.py --help
```

```text
Encrypted Password File Builder

optional arguments:
  -h, --help            show this help message and exit
  -l FILELOCATION, --fileLocation FILELOCATION
                        Password File Location
```

##### Invoking Script for building password files
Script can be invoked with or without the output file location, --fileLocation is an optional parameter.

###### Without --fileLocation parameter
```bash
$ python scripts/encrypted_password_file_builder.py
```

###### With --fileLocation parameter
```bash
$ python scripts/encrypted_password_file_builder.py -l ../password_files
```

##### Decrypt password file to use in python scripts

To decrypt the key.bin and password.bin file. A custom module `password_decryptor.py` have been defined in the script folder. Using the `decrypt_password` method defined in the `password_decryptor.py` module, key.bin and password.bin file will be decrypted to get the password as shown below. Once decrypted the password can be used wherever it is required.
```python
import sys

sys.path.append("../scripts")

from password_decryptor import decrypt_password
password = decrypt_password(key_file_location=r"C:\Users\Kishan\Desktop\python_projects\encrypted_password_file_builder\password_files\key.bin", password_file_location=r"C:\Users\Kishan\Desktop\python_projects\encrypted_password_file_builder\password_files\password.bin")
print(password)
```