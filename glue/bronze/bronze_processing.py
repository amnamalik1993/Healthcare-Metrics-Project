import boto3


# ---------------------------------------
# Configuration
# ---------------------------------------

SOURCE_BUCKET = "heathcare-source-data-bucket"
SOURCE_PREFIX = "raw/"

TARGET_BUCKET = "healthcare-target-data-bucket"
TARGET_PREFIX = "bronze/"


# ---------------------------------------
# Initialize S3 Client
# ---------------------------------------

s3 = boto3.client("s3")


# ---------------------------------------
# List all files in raw/ folder
# ---------------------------------------

response = s3.list_objects_v2(
    Bucket=SOURCE_BUCKET,
    Prefix=SOURCE_PREFIX
)


# ---------------------------------------
# Copy files from raw/ to bronze/
# ---------------------------------------

files = response.get("Contents", [])

print(f"Number of files found: {len(files)}")


for file in files:

    source_key = file["Key"]

    # Skip folder placeholder
    if source_key.endswith("/"):
        continue

    # Extract file name
    file_name = source_key.replace(
        SOURCE_PREFIX,
        "",
        1
    )

    target_key = f"{TARGET_PREFIX}{file_name}"

    print(
        f"Copying: s3://{SOURCE_BUCKET}/{source_key}"
    )

    s3.copy_object(
        CopySource={
            "Bucket": SOURCE_BUCKET,
            "Key": source_key
        },
        Bucket=TARGET_BUCKET,
        Key=target_key
    )

    print(
        f"Copied to: "
        f"s3://{TARGET_BUCKET}/{target_key}"
    )


print("All files copied successfully!")