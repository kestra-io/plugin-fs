ssh-keygen -f src/test/resources/ssh/id_rsa -q -N "" -m PEM -P "testPassPhrase"
ssh-keygen -f src/test/resources/ssh/id_ed25519 -q -n "" -P ""
docker compose -f docker-compose-ci.yml up -d
sleep 3
docker exec plugin-fs-sftp-1 chown -R foo /home/foo/upload