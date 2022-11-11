#!/usr/bin/env bash
set -ex

if [ ! -f VERSION ]; then
    echo "No VERSION file found! Cannot make release!"
    exit 1
else
    echo "VERSION file found..."
fi
VERSION=$(cat VERSION)

# Make sure the version variable is populated
if [ -z "${VERSION}" ]; then
    echo "VERSION file is empty!"
    exit 1
else
    echo "VERSION file contains: ${VERSION}"
fi

# Make sure the version follows the correct format: major.minor.patch
LENGTH_CHECK="${VERSION//[^.]}"
if [ ${#LENGTH_CHECK} != 2 ]; then
    echo "VERSION file contains invalid version (not in format major.minor.patch)"
    exit 1
fi
# Use RegX to ensure it only contains numbers and periods
REGX_CHECK='^([0-9]+\.){0,2}(\*|[0-9]+)$'
if [[ $VERSION =~ $REGX_CHECK ]]; then
    echo "VERSION file contains valid version"
else
    echo "VERSION file contains invalid version (RegX validator failed)"
    exit 1
fi

# FOR TESTING ONLY - hard code version to latest release
VERSION="1.8.10"

PUBLISHED_TAG_VERSION=`npm show aws-iot-device-sdk-v2 version`
if [ "$PUBLISHED_TAG_VERSION" == "$VERSION" ]; then
    echo "$VERSION found in npm. Testing release..."

    # install the Typescript and the SDK
    npm install -g typescript
    npm install

    # Move to the sample folder and download the files there
    cd samples/node/pub_sub
    curl https://www.amazontrust.com/repository/AmazonRootCA1.pem --output ./ca.pem
    cert=$(aws secretsmanager get-secret-value --secret-id "ci/PubSub/cert" --region us-east-1 --query "SecretString" | cut -f2 -d":" | cut -f2 -d\") && echo "$cert" > ./certificate.pem
    key=$(aws secretsmanager get-secret-value --secret-id "ci/PubSub/key" --region us-east-1 --query "SecretString" | cut -f2 -d":" | cut -f2 -d\") && echo "$key" > ./privatekey.pem
    ENDPOINT=$(aws secretsmanager get-secret-value --secret-id "ci/endpoint" --region us-east-1 --query "SecretString" | cut -f2 -d":" | sed -e 's/[\\\"\}]//g')

    # Run the sample!
    npm install
    node dist/index.js --endpoint $ENDPOINT --ca_file './ca.pem' --cert './certificate.pem' --key './privatekey.pem'

    exit 0

else
    echo "$VERSION was not found in npm. Release failed!"
fi

exit 1
