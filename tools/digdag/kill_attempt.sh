#!/bin/bash

DIGDAG=/usr/local/bin/digdag

session_id=$1
get_attempts=`${DIGDAG} attempts ${session_id}`

echo $get_attempts

is_attempt=0
is_id=0
for i in $get_attempts; do

  if [ $is_attempt -eq 1 ] && [ $is_id -eq 1 ]; then
    attempt_id=$i

    #複数回処理をしている場合が、あるので初期化しておく
    is_attempt=0
    is_id=0
  fi

  if [ $i = "attempt" ]; then
    is_attempt=1
  fi
  #attempt idと続く場合にフラグを立てる
  if [ $is_attempt -eq 1 ] && [ $i = "id:" ]; then
    is_id=1
  fi
done

echo $attempt_id

${DIGDAG} kill $attempt_id
