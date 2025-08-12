#!/bin/bash

make compose.down
make processors.up
sleep 2
make processors.purge
sleep 2
make start.dev
make api.test.purge
