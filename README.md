# HTML Downloader 

## What It Is
An **AWS CLOUDFORMATION stack** that **scrapes property web pages at scale**, stores results in S3, and reports progress.

---

## How It Works

1. **Job Distribution**  
   - **SQS** carries batch job messages.  
   - Each message references a **CSV file in S3**.  
   - Each CSV row contains:
     - `parcel_id`
     - `url`
     - `multiValueQueryString`

2. **Scraper Lambda (Node.js)**  
   - Uses **Puppeteer** + **Sparticuz Chromium** to:
     - Build the final URL.
     - Load the web page.
     - Wait until property data is fully loaded.
     - Capture page content.
   - For each **successful row**:
     - Creates a ZIP archive containing:
       - The HTML file (`<parcel_id>.html`)
       - The original single-row `seed.csv`
     - Uploads `<parcel_id>.zip` to S3 under the configured **output prefix**.

3. **Failure Handling**  
   - If **meaningful content** isn’t found:
     - The row is retried (configurable retry count with backoff).
     - After all retries fail:
       - The row is appended to `errors.csv` in S3 (includes original headers).
       - The message is marked for DLQ using **partial batch failure**.

4. **Deploy Timestamp Lambda (Python)**  
   - Periodically updates an environment variable `DEPLOY_TS`.
   - Scheduled to run every **minute** for monitoring and version stamping.

---

## Reliability & Failure Handling

- **Per-row retries** with exponential backoff for “limited data” cases.
- **Partial batch failures** so only bad records are retried.
- Centralized **`errors.csv`** in S3 for failed rows.
- DLQ for unrecoverable failures.

---

## Prerequisites

- AWS CLI configured with appropriate credentials.
- `seed.csv` file with parcel IDs and URLs.

---

## Build & Deploy

```bash
rm -f .aws-sam
sam build -t template.yaml

sam deploy \
  -t .aws-sam/build/template.yaml \
  --stack-name downloader-v2 \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile little-dev \
  --s3-bucket aws-sam-cli-managed-default-samclisourcebucket-u3sewulgdjcm \
  --s3-prefix downloader-v2 \
  --no-confirm-changeset \
  --parameter-overrides \
    ProjectName=downloader-v2 \
    BucketName=my-property-data-pipeline-uploads-aya \
    OutputPrefix=output/html \
    AttachChromiumLayer=false
```

#Post-Deployment Setup

IN THE LAMDA SQS trigget set the concurrency to $35
Batch size: 35 → keeps AWS costs around $20/day.

Can increase up to 150 for faster processing and the daily cost will increase accordingly.

Warning: Going above 150 may risk overloading county websites.





