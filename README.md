## Deployment (AWS SAM)

Use the following commands to build and deploy the stack with AWS SAM:

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




