#!/bin/bash

if [ -e vendor ]; then
    echo "vendor folder already exists"
    exit 1
fi

gb vendor fetch -no-recurse -revision 8eec19e37d25c6568d153517fa0214fbae68a2f1 github.com/riobard/go-mmap
gb vendor fetch -no-recurse -revision afad1794bb13e2a094720aeb27c088aa64564895 github.com/bitly/timer_metrics
