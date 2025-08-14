#!/bin/bash

make compose.down
make processors.up
sleep 1
make processors.purge

make start.dev
sleep 1
make api.test.purge
