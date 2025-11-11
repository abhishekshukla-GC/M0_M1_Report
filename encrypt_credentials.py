"""
Utility script to encrypt credential files for secure storage in repository.

Usage:
    python encrypt_credentials.py

This script will:
1. Encrypt .env, credentials.json, and rsa_key.p8 files
2. Create .env.encrypted, credentials.json.encrypted, and rsa_key.p8.encrypted
3. Generate an encryption key that should be stored in Streamlit secrets

IMPORTANT: Never commit the encryption key to the repository!
Store it in Streamlit Cloud secrets as 'encryption_key'
"""

import os
from pathlib import Path
from cryptography.fernet import Fernet
import base64

def generate_key():
    """Generate a new encryption key."""
    return Fernet.generate_key().decode()

def encrypt_file(input_path: Path, output_path: Path, key: bytes):
    """Encrypt a file using Fernet encryption."""
    fernet = Fernet(key)
    
    if not input_path.exists():
        print(f"Warning: {input_path} does not exist. Skipping...")
        return False
    
    try:
        with open(input_path, 'rb') as f:
            data = f.read()
        
        encrypted_data = fernet.encrypt(data)
        
        with open(output_path, 'wb') as f:
            f.write(encrypted_data)
        
        print(f"✓ Encrypted {input_path.name} → {output_path.name}")
        return True
    except Exception as e:
        print(f"✗ Error encrypting {input_path.name}: {e}")
        return False

def main():
    """Main encryption function."""
    print("=" * 60)
    print("Credential File Encryption Utility")
    print("=" * 60)
    print()
    
    # Files to encrypt
    files_to_encrypt = [
        (Path(".env"), Path(".env.encrypted")),
        (Path("credentials.json"), Path("credentials.json.encrypted")),
        (Path("rsa_key.p8"), Path("rsa_key.p8.encrypted")),
    ]
    
    # Check if encryption key exists in environment or generate new one
    encryption_key = os.getenv("ENCRYPTION_KEY")
    
    if not encryption_key:
        print("No ENCRYPTION_KEY found in environment.")
        print("Generating a new encryption key...")
        encryption_key = generate_key()
        print()
        print("=" * 60)
        print("IMPORTANT: Save this encryption key!")
        print("=" * 60)
        print(f"ENCRYPTION_KEY={encryption_key}")
        print()
        print("Add this to Streamlit Cloud secrets as 'encryption_key'")
        print("For local development, add to .streamlit/secrets.toml:")
        print(f'encryption_key = "{encryption_key}"')
        print("=" * 60)
        print()
        
        # Check if running non-interactively (e.g., in CI/CD or automated script)
        import sys
        if not sys.stdin.isatty():
            print("Running non-interactively. Proceeding with encryption...")
        else:
            response = input("Continue with this key? (y/n): ")
            if response.lower() != 'y':
                print("Aborted.")
                return
    else:
        print("Using encryption key from ENCRYPTION_KEY environment variable.")
        print()
    
    # Convert string key to bytes if needed
    if isinstance(encryption_key, str):
        key_bytes = encryption_key.encode()
    else:
        key_bytes = encryption_key
    
    # Encrypt files
    print("Encrypting files...")
    print()
    encrypted_count = 0
    
    for input_file, output_file in files_to_encrypt:
        if encrypt_file(input_file, output_file, key_bytes):
            encrypted_count += 1
    
    print()
    print("=" * 60)
    print(f"Encryption complete! {encrypted_count} file(s) encrypted.")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Commit the .encrypted files to your repository")
    print("2. Add encryption_key to Streamlit Cloud secrets")
    print("3. Add .env, credentials.json, rsa_key.p8 to .gitignore")
    print("4. DO NOT commit the encryption key to the repository!")

if __name__ == "__main__":
    main()

