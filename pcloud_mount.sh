#!/bin/sh

rclone mount --daemon --vfs-cache-mode full --exclude "/.crypt/"  pcloud: ~/pcloud/

