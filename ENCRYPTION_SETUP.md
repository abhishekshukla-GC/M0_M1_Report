# Credential Encryption Setup Guide

This guide explains how to encrypt your credential files for secure storage in the GitHub repository.

## Overview

The application supports encrypted credential files that can be safely committed to the repository. The encryption key is stored separately in Streamlit Cloud secrets, ensuring that even if someone views the repository code, they cannot access your credentials.

## Files That Can Be Encrypted

- `.env` - Environment variables
- `credentials.json` - Google Sheets service account credentials
- `rsa_key.p8` - Snowflake private key file

## Step-by-Step Encryption Process

### 1. Prepare Your Credential Files

Ensure you have the following files in your project root:
- `.env` (if using environment variables)
- `credentials.json` (Google Sheets credentials)
- `rsa_key.p8` (Snowflake private key)

### 2. Run the Encryption Script

```bash
python encrypt_credentials.py
```

The script will:
1. Check for an existing `ENCRYPTION_KEY` environment variable
2. If not found, generate a new encryption key
3. Encrypt all credential files found
4. Create `.encrypted` versions of each file

### 3. Save the Encryption Key

**IMPORTANT**: The script will display an encryption key. Save this key securely!

```
ENCRYPTION_KEY=your-generated-key-here
```

### 4. Add Encryption Key to Streamlit Cloud

1. Go to your Streamlit Cloud app settings
2. Navigate to "Secrets" section
3. Add the following:

```toml
encryption_key = "your-generated-key-here"
```

### 5. Commit Encrypted Files

Add the encrypted files to your repository:

```bash
git add .env.encrypted
git add credentials.json.encrypted
git add rsa_key.p8.encrypted
git commit -m "Add encrypted credential files"
```

### 6. Update .gitignore

Ensure your `.gitignore` excludes the unencrypted files:

```
.env
credentials.json
rsa_key.p8
.streamlit/secrets.toml
```

## Local Development Setup

For local development, you have two options:

### Option A: Use Encrypted Files (Recommended)

1. Add the encryption key to `.streamlit/secrets.toml`:
   ```toml
   encryption_key = "your-generated-key-here"
   ```

2. Ensure `.streamlit/secrets.toml` is in `.gitignore`

3. The app will automatically decrypt files at runtime

### Option B: Use Plain Files (Development Only)

1. Keep unencrypted files locally (they're in `.gitignore`)
2. Don't commit them to the repository
3. The app will use plain files if encrypted versions aren't found

## How It Works

1. **At Runtime**: The app checks for encrypted files first (`.encrypted` extension)
2. **If Encrypted**: Retrieves the decryption key from Streamlit secrets
3. **Decrypts**: Files are decrypted in memory to temporary files
4. **Uses**: Decrypted credentials are used for database connections
5. **Cleanup**: Temporary files are automatically cleaned up

## Security Best Practices

1. **Never commit** the encryption key to the repository
2. **Never commit** unencrypted credential files
3. **Use different keys** for different environments (dev/staging/prod)
4. **Rotate keys** periodically for enhanced security
5. **Store keys securely** in Streamlit Cloud secrets (encrypted at rest)

## Troubleshooting

### "No decryption key available"

- Ensure `encryption_key` is set in Streamlit Cloud secrets
- For local dev, check `.streamlit/secrets.toml` exists and contains the key

### "Failed to decrypt file"

- Verify the encryption key matches the one used to encrypt
- Ensure encrypted files are not corrupted
- Check file permissions

### "Encrypted file not found"

- Ensure `.encrypted` files are committed to the repository
- Check file paths are correct

## Re-encrypting Files

If you need to update credentials:

1. Update the plain credential files locally
2. Run `encrypt_credentials.py` again (use the same key or generate new one)
3. If using a new key, update Streamlit Cloud secrets
4. Commit the new encrypted files

