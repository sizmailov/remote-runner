#!/bin/bash
# Allow ``ssh localhost`` with empty passphrase on travis-ci.org continuous
# integration platform.

# Make sure we are on Travis.
if [[ ! $TRAVIS ]]; then
    echo "This script is made for travis-ci.org! It cannot run without \$TRAVIS."
    exit 1
fi

# Run SSH service, configure automatic access to localhost.
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N "" -q
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -t rsa localhost >> ~/.ssh/known_hosts
echo '
Host localhost
     IdentityFile ~/.ssh/id_rsa
' >> ~/.ssh/config

