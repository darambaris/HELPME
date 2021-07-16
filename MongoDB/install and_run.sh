#!/bin/bash

# install MongoDB on MacOs using homebrew
# Ref: https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/#std-label-brew-installs-dbtools 
brew tap mongodb/brew 
brew install mongodb-community@5.0

# run MongoDB as a service on MacOs using homebrew
brew services start mongodb-community # service'll run at 127.0.0.1:27017

# run MongoDB on MacOS manually as a background process
mongod --config /usr/local/etc/mongod.conf --fork
