# Streamlit Cloud Deployment Guide

This guide walks you through deploying the MTD Framework app to Streamlit Cloud.

## Prerequisites

1. GitHub account with the repository containing your app
2. Streamlit Cloud account (free tier available)
3. All encrypted credential files committed to the repository
4. Encryption key ready to add to Streamlit secrets

## Step 1: Prepare Your Repository

### 1.1 Ensure Required Files Are Present

Your repository should contain:
- `mtd_app.py` - Main application file
- `queryhelper.py` - Database helper functions
- `requirements.txt` - Python dependencies
- `Template/28 July Run_V2.pptx` - PowerPoint template
- `.streamlit/config.toml` - Streamlit configuration
- Encrypted credential files (`.env.encrypted`, `credentials.json.encrypted`, `rsa_key.p8.encrypted`)

### 1.2 Verify .gitignore

Ensure sensitive files are excluded:
- `.env` (unencrypted)
- `credentials.json` (unencrypted)
- `rsa_key.p8` (unencrypted)
- `.streamlit/secrets.toml` (local secrets)

## Step 2: Deploy to Streamlit Cloud

### 2.1 Connect Your Repository

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository
5. Choose the branch (usually `main` or `master`)
6. Set the main file path: `mtd_app.py`

### 2.2 Configure App Settings

- **App URL**: Choose a unique subdomain
- **Python version**: 3.8 or higher (auto-detected from requirements.txt)

## Step 3: Configure Secrets

### 3.1 Access Secrets Management

1. In your app's settings, click "Secrets"
2. This opens the secrets editor

### 3.2 Add Required Secrets

Copy the structure from `secrets.toml.example` and fill in your values:

```toml
# Encryption key (from encrypt_credentials.py)
encryption_key = "your-encryption-key-here"

# Snowflake Connection
SNOWFLAKE_ACCOUNT = "your-account"
SNOWFLAKE_USER = "your-username"
SNOWFLAKE_WAREHOUSE = "your-warehouse"
SNOWFLAKE_DATABASE = "your-database"
SNOWFLAKE_SCHEMA = "your-schema"
SNOWFLAKE_PRIVATE_KEY_FILE = "rsa_key.p8"
SNOWFLAKE_PRIVATE_KEY_FILE_PWD = "your-key-password"

# PostgreSQL Database URLs
fetch_query_url = "postgresql://user:password@host:port/database"
catalog_url = "postgresql://user:password@host:port/database"
cms_url = "postgresql://user:password@host:port/database"

# Zamzar API Key (for PDF conversion)
zamzar_api_key = "your-zamzar-api-key"
```

### 3.3 Save Secrets

Click "Save" to store your secrets. They are encrypted at rest by Streamlit Cloud.

## Step 4: Deploy and Test

### 4.1 Deploy

1. Click "Deploy" or "Always rerun" if the app is already deployed
2. Wait for the build to complete (usually 2-5 minutes)
3. Check the logs for any errors

### 4.2 Verify Deployment

1. Open your app URL
2. Test the main functionality
3. Verify database connections work
4. Test PDF export (if configured)

## Step 5: Database Access Configuration

### 5.1 Snowflake

Ensure your Snowflake account allows connections from Streamlit Cloud IPs:
- Streamlit Cloud uses dynamic IPs
- You may need to whitelist a range or use network policies
- Check Snowflake documentation for external access configuration

### 5.2 PostgreSQL

If your PostgreSQL database requires IP whitelisting:
- Contact your database administrator
- Provide Streamlit Cloud's IP ranges (if available)
- Or use a VPN/private network solution

## Troubleshooting

### Build Failures

**Issue**: Dependencies not installing
- **Solution**: Check `requirements.txt` for correct package names and versions
- Verify all dependencies are available on PyPI

**Issue**: Import errors
- **Solution**: Ensure all imports are in `requirements.txt`
- Check for typos in module names

### Runtime Errors

**Issue**: "Encryption key not found"
- **Solution**: Verify `encryption_key` is set in Streamlit secrets
- Check the key matches the one used to encrypt files

**Issue**: "Database connection failed"
- **Solution**: Verify database URLs in secrets are correct
- Check database is accessible from external networks
- Verify credentials are correct

**Issue**: "Template not found"
- **Solution**: Ensure `Template/28 July Run_V2.pptx` is in the repository
- Check file path is correct in the code

**Issue**: "PDF conversion failed"
- **Solution**: Verify `zamzar_api_key` is set in secrets
- Check Zamzar API key is valid and has credits
- For local dev, LibreOffice can be used as fallback

### Performance Issues

**Issue**: App is slow
- **Solution**: Optimize database queries
- Use caching for expensive operations (`@st.cache_data`)
- Consider pagination for large datasets

## Updating the App

### Code Updates

1. Push changes to your GitHub repository
2. Streamlit Cloud automatically detects changes
3. App redeploys automatically (or click "Always rerun")

### Secret Updates

1. Go to app settings → Secrets
2. Update the values
3. Click "Save"
4. App restarts automatically

### Template Updates

1. Update `Template/28 July Run_V2.pptx` in your repository
2. Commit and push changes
3. App will use the new template on next deployment

## Security Best Practices

1. **Never commit secrets** to the repository
2. **Use encrypted files** for credentials in the repo
3. **Rotate secrets** periodically
4. **Monitor access logs** in Streamlit Cloud
5. **Use environment-specific secrets** for dev/staging/prod

## Cost Considerations

- **Streamlit Cloud Free Tier**: 
  - Unlimited public apps
  - Private apps require Team plan
  - Resource limits apply

- **Zamzar API**:
  - Free tier: 100 conversions/month
  - Paid plans available for higher usage

## Support

- Streamlit Cloud Docs: https://docs.streamlit.io/streamlit-cloud
- Streamlit Community: https://discuss.streamlit.io
- GitHub Issues: Create an issue in your repository

